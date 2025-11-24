package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
)

// Server handles RabbitMQ server operations
type Server struct {
	middleware      *middleware.Middleware
	logger          *logrus.Logger
	listener        *Listener
	config          config.Interface
	dbClient        *db.Client               // Shared database client
	shutdownHandler ShutdownHandlerInterface // Handles graceful shutdown logic
}

// NewServer creates a new RabbitMQ server
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	mw, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	// Create shared database client
	dbClient, err := db.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create database client: %w", err)
	}

	server := &Server{
		middleware: mw,
		logger:     logger,
		config:     cfg,
		dbClient:   dbClient,
	}

	realFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
		return NewClientManager(cfg, mw, dbClient, publisher)
	}

	listener := NewListener(mw, cfg, dbClient, realFactory)

	server.listener = listener

	// Initialize shutdown handler
	server.shutdownHandler = NewShutdownHandler(
		logger,
		listener,
		mw,
		dbClient,
	)

	logger.WithFields(logrus.Fields{
		"host":        cfg.GetMiddlewareConfig().GetHost(),
		"port":        cfg.GetMiddlewareConfig().GetPort(),
		"user":        cfg.GetMiddlewareConfig().GetUsername(),
		"pod_name":    cfg.GetPodName(),
		"worker_pool": cfg.GetWorkerPoolSize(),
	}).Info("Server initialized successfully")

	return server, nil
}

func (s *Server) Run() error {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	serverDone := s.startServerGoroutine()

	return s.shutdownHandler.HandleShutdown(serverDone, osSignals)
}

func (s *Server) startServerGoroutine() chan error {
	serverDone := make(chan error, 1)
	go func() {
		s.logger.WithFields(logrus.Fields{
			"pod_name":    s.config.GetPodName(),
			"worker_pool": s.config.GetWorkerPoolSize(),
		}).Info("Starting data dispatcher service")

		err := s.startComponents()
		serverDone <- err
	}()
	return serverDone
}

// main function
func (s *Server) startComponents() error {
	err := s.listener.Start() // this is the main blocking call
	if err != nil {
		if err.Error() == "context canceled" {
			s.logger.Info("Listener stopped consuming gracefully.")
			return nil
		}
		return fmt.Errorf("failed to start consuming: %w", err)
	}
	return nil
}
