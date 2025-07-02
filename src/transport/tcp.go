package transport

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/mlops-eval/data-dispatcher-service/src/connection"
)

type TCPServer struct {
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

func NewTCPServer(addr string) *TCPServer {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Error al listen TCP: %v", err)
	}
	return &TCPServer{listener: l, quit: make(chan struct{})}
}

func (s *TCPServer) Start() {
	s.wg.Add(1)
	defer s.wg.Done()
	log.Printf("Starting the server")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("Error en accept: %v", err)
				continue
			}
		}
		s.wg.Add(1)
		go func() {
			// Generate a unique clientID for this connection
			clientID := fmt.Sprintf("client_%s", conn.RemoteAddr().String())
			connection.Handle(conn, clientID)
			s.wg.Done()
		}()
	}
}

func (s *TCPServer) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}
