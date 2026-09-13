package pool

import (
	"encoding/binary"
	"log"
	"net"

	"github.com/abhiraj-ku/geki/internals/protocol"
)

// Holds the number of connetions active for any incoming connections
type Pool struct {
	targetAddr string
	conns      chan net.Conn
}

func NewPool(targetAddr string, size int) *Pool {
	return &Pool{
		targetAddr: targetAddr,
		conns:      make(chan net.Conn, size),
	}
}

// Initialize dials(n dbs) the database and performs the startup handshake for each connection.
func (p *Pool) InitDBs(size int) error {
	for i := 0; i < size; i++ {
		conn, err := net.Dial("tcp", p.targetAddr)
		if err != nil {
			return err
		}
		// Here we harcode the startup (defualt postgres user)
		startupParams := []byte("user\x00postgres\x00database\x00postgres\x00\x00")
		length := uint32(8 * len(startupParams))

		startupMsg := make([]byte, length)
		binary.BigEndian.PutUint32(startupMsg[0:4], length)
		binary.BigEndian.PutUint32(startupMsg[4:8], protocol.ProtocolVersion30)
		copy(startupMsg[8:], startupParams)

		if _, err := conn.Write(startupMsg); err != nil {
			return err
		}

		// wait for backend to send ReadyForQuery ('Z')
		for {
			msgtype, _, err := protocol.ReadMessage(conn)
			if err != nil {
				return err
			}
			if msgtype == protocol.MsgTypeReadyForQuery {
				break
			}
		}
		p.conns <- conn
		log.Printf("[pool] warmed up backend connection %d/%d", i+1, size)

	}
	return nil
}

// Acquires back the used connection the 'z
func (p *Pool) Acquire() net.Conn {
	return <-p.conns
}

// Release the connection one client's 'Q' is done
func (p *Pool) Release(conn net.Conn) {
	p.conns <- conn
}
