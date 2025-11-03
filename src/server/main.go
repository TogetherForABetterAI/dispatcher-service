package server

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	"github.com/sirupsen/logrus"
)

type Orchestrator interface {
	RequestShutdown()
	RequestScaleUp()
}

// Server handles RabbitMQ server operations
type Server struct {
	middleware      *middleware.Middleware
	logger          *logrus.Logger
	listener        *Listener
	monitor         *ReplicaMonitor
	config          config.Interface
	shutdownRequest chan struct{}         // Channel to receive the shutdown request
	shutdownOnce    sync.Once             // Ensures Stop() is called only once
	scalePublisher  *middleware.Publisher // Publisher for scaling requests
}

// NewServer creates a new RabbitMQ server
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	mw, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	shutdownReqChan := make(chan struct{}, 1)

	publisher, err := middleware.NewPublisher(mw.Conn())

	if err != nil {
		mw.Close()
		return nil, fmt.Errorf("failed to create scale publisher: %w", err)
	}

	server := &Server{
		middleware:      mw,
		logger:          logger,
		config:          cfg,
		shutdownRequest: shutdownReqChan,
		scalePublisher:  publisher,
	}

	monitor := NewReplicaMonitor(cfg, logger, server)

	realFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
		return NewClientManager(cfg, mw, clientID)
	}

	listener := NewListener(mw, cfg, monitor, realFactory)

	server.monitor = monitor
	server.listener = listener

	logger.WithFields(logrus.Fields{
		"host": cfg.GetMiddlewareConfig().GetHost(),
		"port": cfg.GetMiddlewareConfig().GetPort(),
		"user": cfg.GetMiddlewareConfig().GetUsername(),
	}).Info("Server initialized")

	return server, nil
}

// main function
func (s *Server) Start() error {
	s.monitor.Start()
	err := s.listener.Start()
	if err != nil {
		if err.Error() == "context canceled" {
			s.logger.Info("Listener stopped consuming gracefully.")
			return nil
		}
		return fmt.Errorf("failed to start consuming: %w", err)
	}
	return nil
}

// RequestShutdown allows the ReplicaMonitor to request a server shutdown
func (s *Server) RequestShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownRequest)
	})
}

func (s *Server) GetShutdownChan() chan struct{} {
	return s.shutdownRequest
}

// RequestScaleUp allows the ReplicaMonitor to request scaling up a service type
func (s *Server) RequestScaleUp() {

	msg := models.ScaleMessage{ReplicaType: s.config.GetReplicaName()}
	body, err := json.Marshal(msg)
	if err != nil {
		s.logger.WithField("error", err).Error("Failed to marshal scale-up message")
		return
	}
	err = s.scalePublisher.Publish(
		"",
		body,
		config.SCALABILITY_EXCHANGE,
	)
	if err != nil {
		s.logger.WithField("error", err).Error("Failed to publish scale-up message")
	}
}

// ShutdownClients initiates the server shutdown
func (s *Server) ShutdownClients(interrupt bool) {
	s.logger.Info("Initiating server shutdown...")
	s.middleware.StopConsuming(s.listener.GetConsumerTag())
	s.monitor.Stop()
	if interrupt {
		// interrupts ongoing processing
		s.listener.InterruptClients(true)
	} else {
		// If desired, a timeout could be added to set a limit
		// on how long we wait for clients to finish.
		s.listener.InterruptClients(false)
	}
	s.middleware.Close()

	s.logger.Info("Server stopped consuming, all clients finished.")
}
