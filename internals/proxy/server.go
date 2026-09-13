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
// Role of the server is the middleman , accepting connections
// handing it over to session
type Server struct {
	listenAdd   string
	targetAddr  string
	backendPool *pool.Pool
	wg          sync.WaitGroup
}

func NewServer(listenAddr, targetAddr string) *Server {
	return &Server{
		listenAdd:  listenAddr,
		targetAddr: targetAddr,
	}
}

// starts the TCP listener
func (s *Server) Start(ctx context.Context) error {
	// inits the backedn pool with 3 active conns
	s.backendPool = pool.NewPool(s.targetAddr, 3)
	if err := s.backendPool.InitDBs(3); err != nil {
		return err
	}

	lsn, err := net.Listen("tcp", s.listenAdd)
	if err != nil {
		return err
	}
	defer lsn.Close()

	log.Printf("[Server] Proxy listening on %s", s.listenAdd)

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
			session := NewSession(c, s.backendPool)
			session.Run()
		}(clientConn)

	}
}
func (s *Server) Wait() {
	s.wg.Wait()
}
