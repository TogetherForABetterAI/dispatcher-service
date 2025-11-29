package server

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// ShutdownHandlerInterface defines the interface for handling graceful shutdown
type ShutdownHandlerInterface interface {
	// HandleShutdown orchestrates the shutdown process
	// Returns an error if shutdown encounters an issue
	HandleShutdown(serverDone chan error, osSignals chan os.Signal) error

	// ShutdownClients initiates client shutdown with interruption
	ShutdownClients()
}

// ShutdownHandler implements the ShutdownHandlerInterface interface
type ShutdownHandler struct {
	logger     *logrus.Logger
	listener   ListenerInterface
	middleware ShutdownMiddleware
	dbCloser   DBCloser
	wg         sync.WaitGroup
}

// ShutdownMiddleware defines the middleware methods needed for shutdown
type ShutdownMiddleware interface {
	StopConsuming(consumerTag string) error
	Close()
}

// DBCloser defines the interface for closing database connections
type DBCloser interface {
	Close()
}

// ListenerInterface defines the listener methods needed for shutdown
type ListenerInterface interface {
	GetConsumerTag() string
	InterruptClients()
}

// NewShutdownHandler creates a new shutdown handler
func NewShutdownHandler(
	logger *logrus.Logger,
	listener ListenerInterface,
	middleware ShutdownMiddleware,
	dbCloser DBCloser,
) ShutdownHandlerInterface {
	return &ShutdownHandler{
		logger:     logger,
		listener:   listener,
		middleware: middleware,
		dbCloser:   dbCloser,
	}
}

// HandleShutdown orchestrates graceful shutdown based on shutdown sources
func (h *ShutdownHandler) HandleShutdown(serverDone chan error, osSignals chan os.Signal) error {
	// Wait for one of two shutdown triggers:
	// 1. Server error/completion (serverDone)
	// 2. OS signal (SIGTERM/SIGINT from Kubernetes or user)
	select {
	case err := <-serverDone:
		// Server stopped (error or normal completion)
		h.logger.Info("Server stopped, initiating shutdown")
		close(osSignals) // Signal OS goroutine to stop if it's listening
		h.ShutdownClients()
		return h.handleServerError(err)

	case sig, ok := <-osSignals:
		// OS signal received (SIGTERM from Kubernetes operator or SIGINT from user)
		if !ok {
			return nil
		}
		h.logger.WithField("signal", sig).Info("Received OS signal, initiating shutdown")
		h.ShutdownClients()

		// Wait for server to finish after interrupting clients
		err := <-serverDone
		return h.handleServerError(err)
	}
}

// handleServerError handles shutdown when server stops
func (h *ShutdownHandler) handleServerError(err error) error {
	if err != nil {
		h.logger.WithError(err).Error("Service stopped with an error")
		return err
	}
	h.logger.Info("Service stopped cleanly")
	return nil
}

// ShutdownClients initiates the shutdown of all server components
// Always interrupts ongoing processing via context cancellation
func (h *ShutdownHandler) ShutdownClients() {
	h.logger.Info("Shutting down server components...")

	// Stop consuming new messages from RabbitMQ
	if err := h.middleware.StopConsuming(h.listener.GetConsumerTag()); err != nil {
		h.logger.WithError(err).Error("Error stopping consumer")
	}

	// Interrupt all active clients by canceling their contexts
	// This triggers graceful shutdown: workers finish current jobs, then stop
	h.listener.InterruptClients()

	// Close middleware connection
	h.middleware.Close()

	// Close database connection pool
	h.dbCloser.Close()

	h.logger.Info("Server shutdown complete")
}
