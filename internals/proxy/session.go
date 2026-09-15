package proxy

import (
	"log"
	"net"
	"strings"

	"github.com/abhiraj-ku/geki/internals/pool"
	"github.com/abhiraj-ku/geki/internals/protocol"
)

// Session now completes the client hanshake immediately
// sits idele in loop waiting for query to arrive
// borrows the conenction from the pool
type Session struct {
	clientConn  net.Conn
	primaryPool *pool.Pool
	replicaPool *pool.Pool

	// pinnedConn holds the primary conn if the client is in transaction
	pinnedConn net.Conn
}

func NewSession(clientConn net.Conn, primaryPool, replicaPool *pool.Pool) *Session {
	return &Session{
		clientConn:  clientConn,
		primaryPool: primaryPool,
		replicaPool: replicaPool,
	}
}

// isReadOnly tells if the current client query is read (select) query
func isReadOnly(query string) bool {
	q := strings.TrimSpace(strings.ToUpper(query))
	if strings.HasPrefix(q, "SELECT") && !strings.Contains(q, "FOR UPDATE") {
		return true
	}
	return false
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
	s.loopQueries()

}

// loopQueries runs a infinte loop the session and runs the query forwards thing
func (s *Session) loopQueries() {
	// ensure the pinned connection is releasef if client drops in between
	defer s.releasePinned()

	for {
		msgtype, clientPayload, err := protocol.ReadMessage(s.clientConn)
		if err != nil {
			log.Printf("[Session] Client %s disconnected: %v", s.clientConn.RemoteAddr(), err)
			return
		}
		if msgtype == 'X' { // 'X' is terminated
			log.Printf("[Session] Client %s terminated the session", s.clientConn.RemoteAddr())
			return
		}

		var backendConn net.Conn
		var fromReplica bool

		// routing and pinned connection decision
		if s.pinnedConn != nil {
			// in a transaction as pinned is not free , force the traffic to primary pool
			backendConn = s.pinnedConn
			fromReplica = false
		} else {
			// not in transaction , so we need to evaluate the query
			if msgtype == protocol.MsgTypeQuery {
				queryStr := string(clientPayload[:len(clientPayload)-1])
				if isReadOnly(queryStr) {
					backendConn = s.replicaPool.Acquire()
					fromReplica = true
					log.Printf("[router] -> Replica: %s", queryStr)
				} else {
					backendConn = s.primaryPool.Acquire()
					fromReplica = false
					log.Printf("[router] -> Primary: %s", queryStr)
				}
			} else {
				// default to primary for non-simple query protocol messages (Parse, Bind, Execute)
				backendConn = s.primaryPool.Acquire()
				fromReplica = false
			}

		}
		// forward the query
		if err := protocol.WriteMessage(backendConn, msgtype, clientPayload); err != nil {
			return
		}

		// stream back the response and track txn state
		for {
			bType, bPayload, err := protocol.ReadMessage(backendConn)
			if err != nil {
				return
			}
			if err := protocol.WriteMessage(s.clientConn, bType, bPayload); err != nil {
				return
			}
			// The postgres state machine to handle the query types
			if bType == protocol.MsgTypeReadyForQuery {
				txnStatus := bPayload[0] // 'I' (idle) , 'T' (in transaction) , 'E' (Error transaction)

				if txnStatus == 'T' || txnStatus == 'E' {
					// postgres confirms we are inside the transaction
					// pin the connection to backend
					s.pinnedConn = backendConn
				} else if txnStatus == 'I' {
					// Postgres confirms we are idle state , unpin and return the correct pool
					s.pinnedConn = nil
					if fromReplica {
						s.replicaPool.Release(backendConn)
					} else {
						s.primaryPool.Release(backendConn)
					}
				}
				break // wait for clients next command

			}
		}

	}

}

func (s *Session) releasePinned() {
	if s.pinnedConn != nil {
		s.primaryPool.Release(s.pinnedConn)
		s.pinnedConn = nil
	}
}
