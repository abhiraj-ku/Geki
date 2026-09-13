package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// handles reading and writing raw PostgreSQL packets using big-endian binary decoding,
// rejecting SSL probes, and parsing the key-value startup parameters

// These are standard postgres wire protocol headers and auth types
const (
	SSLRequestCode    uint32 = 80877103 // 0x04D2162F
	GSSENCRequestCode uint32 = 80877104 // 0x04D21630
	ProtocolVersion30 uint32 = 196608   // 3.0: (3 << 16)
	MaxPacketLength   uint32 = 16 * 1024 * 1024

	// Message Types
	MsgTypeAuth            byte = 'R'
	MsgTypePassword        byte = 'p'
	MsgTypeParameterStatus byte = 'S'
	MsgTypeBackendKeyData  byte = 'K'
	MsgTypeReadyForQuery   byte = 'Z'
	MsgTypeError           byte = 'E'
	MsgTypeQuery           byte = 'Q'
)

// this represents the initial client connection parameters
type StartupMessage struct {
	ProtocolVersion uint32
	Parameters      map[string]string
	RawBytes        []byte
}

// ReadStartupHandshake intercepts SSL/GSS probes and parses the StartupMessage.
func ReadStartupHandshake(clientConn net.Conn) (*StartupMessage, error) {
	for {
		// 1. Read the 4-byte length prefix
		var length uint32
		if err := binary.Read(clientConn, binary.BigEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read message length: %w", err)
		}

		if length < 8 {
			return nil, fmt.Errorf("invalid packet length: %d", length)
		}
		if length > MaxPacketLength {
			return nil, fmt.Errorf("packet length %d exceeds maximum %d", length, MaxPacketLength)
		}

		// 2. Read protocol code / version (4 bytes)
		var code uint32
		if err := binary.Read(clientConn, binary.BigEndian, &code); err != nil {
			return nil, fmt.Errorf("failed to read protocol code: %w", err)
		}

		// 3. Handle SSL or GSS probes: reply 'N' to force plain TCP
		if code == SSLRequestCode || code == GSSENCRequestCode {
			if err := writeFull(clientConn, []byte{'N'}); err != nil {
				return nil, fmt.Errorf("failed to write SSL/GSS rejection: %w", err)
			}
			continue // Client will now send the real StartupMessage
		}

		// 4. Read remaining payload (length includes itself and code: length - 8 bytes)
		payload := make([]byte, length-8)
		if _, err := io.ReadFull(clientConn, payload); err != nil {
			return nil, fmt.Errorf("failed to read startup payload: %w", err)
		}

		// Reconstruct raw bytes to forward directly to Postgres backend
		raw := make([]byte, length)
		binary.BigEndian.PutUint32(raw[0:4], length)
		binary.BigEndian.PutUint32(raw[4:8], code)
		copy(raw[8:], payload)

		params, err := parseStartupParameters(payload)
		if err != nil {
			return nil, fmt.Errorf("invalid startup parameters: %w", err)
		}

		return &StartupMessage{
			ProtocolVersion: code,
			Parameters:      params,
			RawBytes:        raw,
		}, nil
	}
}

// This extracts null-terminated key-value pairs
func parseStartupParameters(payload []byte) (map[string]string, error) {
	params := make(map[string]string)
	parts := bytes.Split(payload, []byte{0})

	// Format: key\0value\0key\0value\0\0
	terminator := -1
	for i, part := range parts {
		if len(part) == 0 {
			terminator = i
			break
		}
	}
	if terminator == -1 || terminator%2 != 0 || terminator+1 >= len(parts) {
		return nil, fmt.Errorf("missing parameter terminator")
	}
	for _, part := range parts[terminator+1:] {
		if len(part) != 0 {
			return nil, fmt.Errorf("data after parameter terminator")
		}
	}
	for i := 0; i < terminator; i += 2 {
		key := string(parts[i])
		if key == "" {
			return nil, fmt.Errorf("empty parameter name")
		}
		val := string(parts[i+1])
		params[key] = val
	}
	return params, nil
}

// This reads standard Postgres framing: [1-byte Type][4-byte Length][Payload]
func ReadMessage(r io.Reader) (byte, []byte, error) {
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return 0, nil, err
	}
	msgType := typeBuf[0]

	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, nil, err
	}

	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", length)
	}
	if length > MaxPacketLength {
		return 0, nil, fmt.Errorf("message length %d exceeds maximum %d", length, MaxPacketLength)
	}

	// Payload excludes the 4 length bytes
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}

// WriteMessage serializes a standard framed message
func WriteMessage(w io.Writer, msgType byte, payload []byte) error {
	if len(payload) > int(MaxPacketLength)-4 {
		return fmt.Errorf("message payload length %d exceeds maximum %d", len(payload), MaxPacketLength-4)
	}
	length := uint32(len(payload) + 4)

	buf := make([]byte, 5+len(payload))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], length)
	copy(buf[5:], payload)

	return writeFull(w, buf)
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if n > 0 {
			buf = buf[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Now we imitate a fake connection to the client (avoding full scram-sha256 clinet from
// scratch is expensive)

// this function tells the client auth was ok instead of connecting
func WriteAuthOk(w io.Writer) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, 0)
	return WriteMessage(w, MsgTypeAuth, payload)
}

// This tells client the proxy is ready to accept the command
func ReadyForQuery(w io.Writer, status byte) error {
	return WriteMessage(w, MsgTypeReadyForQuery, []byte{status})
}
