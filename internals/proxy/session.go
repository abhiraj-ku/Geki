package proxy

import (
	"io"
	"log"
	"net"
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

	// dial to actual postgres server via tcp conn
	postgreConn, err := net.Dial("tcp", s.targetAddr)
	if err != nil {
		log.Printf("[session] failed to conn to pg server: %w", err)

	}
	defer postgreConn.Close()

	log.Printf("[Session] Proxying %s <-> %s", s.clientConn.RemoteAddr(), postgreConn.RemoteAddr())

	// error channel to know when one pipe fials or crashes
	errc := make(chan error, 2)

	// Pipe imitates the real client <-> server connection talk
	// like real client when connected to running instance of pg server via psql
	// our server will create a new sesion for each client connecting to this proxy server
	// todo: make custom message buffer for the incoming request , intercept it and then proxy to
	// logical replicas (read or write based on SELECT or INSERT/UPDATE query)

	// pipe client -> postgres
	go func() {
		// copy the client's "command" to the destination postgres server
		// as of now we will use psql to connect to pg server
		// client send something like SELECT NOW();
		// it will remove this later when we build our custom buffer and message interceptor
		_, err := io.Copy(postgreConn, s.clientConn)
		errc <- err
	}()

	// pipe postgres -> client
	go func() {
		_, err := io.Copy(s.clientConn, postgreConn)
		errc <- err
	}()

	// wait for any one to report something
	<-errc
	log.Printf("[Session] Closed %s", s.clientConn.RemoteAddr())
}
