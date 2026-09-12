package proxy

import (
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
func (s *Session) authReady(pgConn net.Conn) error {}

// contiue loop on active queries
func (s *Session) loopIncomingQueries(pgConn net.Conn) error {}
