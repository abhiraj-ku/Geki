package proxy

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/abhiraj-ku/geki/internals/pool"
)

// Proxy server spaws a new Session for each new client
// listens on port 5433 and when any client connects to this
// the server hands it over to the Session to actually connect to
// to the pg and handles it

// V2 : Now we route traffic based on the statement type to primary or replica db
type Server struct {
	listenAddr  string
	primaryAddr string
	replicaAddr string
	primaryPool *pool.Pool
	replicaPool *pool.Pool
	wg          sync.WaitGroup
}

func NewServer(listenAddr, primaryAddr, replicaAddr string) *Server {
	return &Server{
		listenAddr:  listenAddr,
		primaryAddr: primaryAddr,
		replicaAddr: replicaAddr,
	}
}

// starts the TCP listener
func (s *Server) Start(ctx context.Context) error {
	// inits the primary pool with 3 active conns
	s.primaryPool = pool.NewPool(s.primaryAddr, 5)
	if err := s.primaryPool.InitDBs(5); err != nil {
		return err
	}

	// initilazize the replica pool
	s.replicaPool = pool.NewPool(s.replicaAddr, 10)
	if err := s.replicaPool.InitDBs(10); err != nil {
		return err
	}

	lsn, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer lsn.Close()

	log.Printf("[Server] Proxy listening on %s", s.listenAddr)
	log.Printf("[Server] Primary: %s | Replica: %s", s.primaryAddr, s.replicaAddr)
	// Graceful shutdown
	go func() {
		<-ctx.Done()
		log.Println("[server] shutting down the server...")
		lsn.Close()
	}()

	// Accept the connection request "intercepted" by lisnter above infintely
	for {
		clientConn, err := lsn.Accept()
		// check if request is cancelled : context timedout or what
		if err != nil {
			// if context is cancelled
			if ctx.Err() != nil {
				return nil
			}
			fmt.Printf("[server] connection accept error: %v", err)
			continue
		}

		// start the session in new goroutine so server keeps on accepting new connection
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			session := NewSession(c, s.primaryPool, s.replicaPool)
			session.Run()
		}(clientConn)

	}
}
func (s *Server) Wait() {
	s.wg.Wait()
}
