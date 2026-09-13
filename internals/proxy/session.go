package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/abhiraj-ku/geki/internals/protocol"
)

// Represents the single client connection to proxy
type Session struct {
	clientConn net.Conn
	targetAddr string
}

func NewSession(clientConn net.Conn, targetAddr string) *Session {
	return &Session{
		clientConn: clientConn,
		targetAddr: targetAddr,
	}
}

// This method connects to postgres and pipes data bidirectional
// postgres <-> client
// client <-> postgres
// For each client we will spun a new sessions
// both will communicate via this pipe to share or exhange data
func (s *Session) Run() {
	defer s.clientConn.Close()

	// Read and intercept the client handshake request with our
	startup, err := protocol.ReadStartupHandshake(s.clientConn)
	if err != nil {
		log.Printf("[Session %s] Startup error: %v", s.clientConn.RemoteAddr(), err)
		return
	}

	// intercepted client's log
	log.Printf("[Handshake] Client connected: user=%q db=%q app=%q",
		startup.Parameters["user"],
		startup.Parameters["database"],
		startup.Parameters["application_name"],
	)

	// dial to actual postgres server via tcp conn
	postgreConn, err := net.Dial("tcp", s.targetAddr)
	if err != nil {
		fmt.Printf("[session] failed to conn to pg server: %v", err)
		return

	}
	defer postgreConn.Close()

	log.Printf("[Session] Proxying %s <-> %s", s.clientConn.RemoteAddr(), postgreConn.RemoteAddr())

	// forward the intercepted message directly to postgres instad of copying via io and spinning gorotoines now
	if _, err := postgreConn.Write(startup.RawBytes); err != nil {
		log.Printf("[session] failed to conn to pg server: %v", err)
		return
	}

	// relay the auth and init pg <-> client
	if err := s.authReady(postgreConn); err != nil {
		log.Printf("[session] handshake failed: %v", err)
		return

	}

	log.Printf("[Session] Handshake complete. Steady state ready.")

	// loop on the active queries
	s.loopIncomingQueries(postgreConn)

}

// Handle the authentication handshake and initialization
func (s *Session) authReady(pgConn net.Conn) error {
	for {
		msgTypes, payload, err := protocol.ReadMessage(pgConn)
		if err != nil {
			return fmt.Errorf("[session] failed to read messages: %w", err)
		}
		switch msgTypes {
		case protocol.MsgTypeAuth:
			if len(payload) < 4 {
				return errors.New("message type malfunctioned")
			}
			authType := binary.BigEndian.Uint32(payload[:4])
			log.Printf("[session-auth] auth challenge from postgres type:%d", authType)

			// forward the challenge to client
			if err := protocol.WriteMessage(s.clientConn, msgTypes, payload); err != nil {
				return err
			}

			// AuthenticationOk (0) completes authentication. For other auth
			// methods, the client must answer the challenge.
			if authType != 0 {
				cType, cPayload, err := protocol.ReadMessage(s.clientConn)
				if err != nil {
					return fmt.Errorf("client auth response error: %w", err)
				}
				if err := protocol.WriteMessage(pgConn, cType, cPayload); err != nil {
					return err
				}

			}
		case protocol.MsgTypeReadyForQuery:
			// handshake is officaly finished
			log.Printf("[handshake] ReadyForQuery recived (status: %c)", payload[0])
			return protocol.WriteMessage(s.clientConn, msgTypes, payload)
		case protocol.MsgTypeError:
			_ = protocol.WriteMessage(s.clientConn, msgTypes, payload)
			return errors.New("database returned error during handhsake")

		default:
			if err := protocol.WriteMessage(s.clientConn, msgTypes, payload); err != nil {
				return err
			}
		}
	}
}

// contiue loop on active queries
func (s *Session) loopIncomingQueries(pgConn net.Conn) {
	errc := make(chan error, 2)

	// client -> backend (inspect simple queries)
	go func() {
		for {
			msgtype, payload, err := protocol.ReadMessage(s.clientConn)
			if err != nil {
				errc <- err
				return
			}

			// intercept simple query (Q)
			if msgtype == protocol.MsgTypeQuery && len(payload) > 0 {
				queryString := string(payload[:len(payload)-1]) // drop the trailing null byte
				log.Printf("[Layer-7 Intercept] SQL: %s", queryString)
			}
			if err := protocol.WriteMessage(pgConn, msgtype, payload); err != nil {
				errc <- err
				return
			}

		}
	}()

	// Backend -> client

	go func() {
		for {
			msgtype, payload, err := protocol.ReadMessage(pgConn)
			if err != nil {
				errc <- err
				return
			}
			if err := protocol.WriteMessage(s.clientConn, msgtype, payload); err != nil {
				errc <- err
				return
			}
		}

	}()
	<-errc
	log.Printf("[Session] Terminated connection %s", s.clientConn.RemoteAddr())
}
