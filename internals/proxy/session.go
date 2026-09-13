package proxy

import (
	"log"
	"net"

	"github.com/abhiraj-ku/geki/internals/pool"
	"github.com/abhiraj-ku/geki/internals/protocol"
)

// Session now completes the client hanshake immediately
// sits idele in loop waiting for query to arrive
// borrows the conenction from the pool
type Session struct {
	clientConn  net.Conn
	backendPool *pool.Pool
}

func NewSession(clientConn net.Conn, backendPool *pool.Pool) *Session {
	return &Session{
		clientConn:  clientConn,
		backendPool: backendPool,
	}
}

func (s *Session) Run() {
	defer s.clientConn.Close()

	// Read and intercept the client handshake request with our
	_, err := protocol.ReadStartupHandshake(s.clientConn)
	if err != nil {
		log.Printf("[Session %s] Startup error: %v", s.clientConn.RemoteAddr(), err)
		return
	}

	// fake the client authentication locally
	if err := protocol.WriteAuthOk(s.clientConn); err != nil {
		return
	}
	if err := protocol.ReadyForQuery(s.clientConn, 'I'); err != nil {
		return
	}
	log.Printf("[Session] Client %s connected & suspended in idle state", s.clientConn.RemoteAddr())

	// loop on the active queries
	s.loopIncomingQueries()

}

func (s *Session) loopIncomingQueries() {
	for {
		// wait for client to actually send the client
		msgtypes, clientPayload, err := protocol.ReadMessage(s.clientConn)
		if err != nil {
			return
		}
		// if client send the query to close 'X'
		if msgtypes == 'X' {
			return
		}

		if msgtypes == protocol.MsgTypeQuery {
			queryStr := string(clientPayload[:len(clientPayload)-1])
			log.Printf("[Session] client executing: %s", queryStr)
		}

		// Connection pooling starts here

		// checkout connections from pool
		backendConn := s.backendPool.Acquire()

		// forward the query to backend
		if err := protocol.WriteMessage(backendConn, msgtypes, clientPayload); err != nil {
			log.Printf("[session] backen writes failed: %v", err)
			s.backendPool.Release(backendConn)
			return
		}

		// Now stream the backend response untill we see 'Z' readyforQuery
		for {
			bType, bPayload, err := protocol.ReadMessage(backendConn)
			if err != nil {
				log.Printf("[session] backend read failed: %v", err)
				// we do not return the connection to pool if broker
				return
			}
			// forward the response to client
			if err := protocol.WriteMessage(s.clientConn, bType, bPayload); err != nil {
				s.backendPool.Release(backendConn)
				return
			}

			// check back if client is done with query
			if bType == protocol.MsgTypeReadyForQuery {
				s.backendPool.Release(backendConn)
				break // wait for new client
			}
		}
	}
}
