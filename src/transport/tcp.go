package transport

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/mlops-eval/data-dispatcher-service/src/connection"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
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

	middleware, err := middleware.NewMiddleware()
	if err != nil {
		log.Fatalf("Error al crear el middleware: %v", err)
	}
	if err := middleware.DeclareExchange("data_exchange", "direct"); err != nil {
		log.Fatalf("Error al declarar el exchange: %v", err)
	}

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
			connection.Handle(conn, clientID, middleware)
			s.wg.Done()
		}()
	}
}

func (s *TCPServer) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}
