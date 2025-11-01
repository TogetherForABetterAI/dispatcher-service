package server

import (
	"fmt"
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
)

// Server handles RabbitMQ server operations for client notifications
type Server struct {
	middleware *middleware.Middleware
	logger     *logrus.Logger
	listener   *Listener
	config     config.Interface
}

// NewServer creates a new RabbitMQ server for client notifications
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Use middleware to establish RabbitMQ connection
	middleware, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	// Create client manager with shared connection
	clientManager := NewClientManager(cfg, middleware.Conn(), middleware)

	// Create listener
	listener := NewListener(clientManager, middleware, cfg)

	server := &Server{
		middleware: middleware,
		logger:     logger,
		listener:   listener,
		config:     cfg,
	}

	logger.WithFields(logrus.Fields{
		"host": cfg.GetMiddlewareConfig().GetHost(),
		"port": cfg.GetMiddlewareConfig().GetPort(),
		"user": cfg.GetMiddlewareConfig().GetUsername(),
	}).Info("Server initialized - ready to consume from data-dispatcher-connections queue")

	return server, nil
}

// Start starts consuming client notification messages from the existing queue
func (s *Server) Start() error {
	// Start the listener to consume messages 
	// spawn goroutines for each connection packet received
	err := s.listener.Start()
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	return nil
}

// Stop gracefully stops the server and waits for all client goroutines to finish
func (s *Server) Stop() {
	s.logger.Info("Initiating graceful server shutdown")

	s.middleware.StopConsuming(s.listener.GetConsumerTag())

	// Signal all client goroutines to stop
	s.listener.Stop()

	// Close channel and connection
	s.middleware.Close()

	s.logger.Info("Server shutdown completed")
}
