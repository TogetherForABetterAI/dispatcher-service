package server

import (
	"github.com/sirupsen/logrus"
	"log/slog"
	"os"
)

// ShutdownHandlerInterface defines the interface for handling graceful shutdown
type ShutdownHandlerInterface interface {
	// HandleShutdown orchestrates the shutdown process
	// Returns an error if shutdown encounters an issue
	HandleShutdown(serverDone chan error, osSignals chan os.Signal) error

	// ShutdownClients initiates client shutdown
	// interrupt=true will forcefully interrupt ongoing processing
	// interrupt=false will wait for clients to finish gracefully
	ShutdownClients(interrupt bool)
}

// ShutdownHandler implements the ShutdownHandlerInterface interface
type ShutdownHandler struct {
	logger          *logrus.Logger
	listener        ListenerInterface
	monitor         ReplicaMonitorInterface
	middleware      ShutdownMiddleware
	shutdownRequest chan struct{}
}

// ShutdownMiddleware defines the middleware methods needed for shutdown
type ShutdownMiddleware interface {
	StopConsuming(consumerTag string) error
	Close()
}

// ListenerInterface defines the listener methods needed for shutdown
type ListenerInterface interface {
	GetConsumerTag() string
	InterruptClients(interrupt bool)
}

// NewShutdownHandler creates a new shutdown handler
func NewShutdownHandler(
	logger *logrus.Logger,
	listener ListenerInterface,
	monitor ReplicaMonitorInterface,
	middleware ShutdownMiddleware,
	shutdownRequest chan struct{},
) ShutdownHandlerInterface {
	return &ShutdownHandler{
		logger:          logger,
		listener:        listener,
		monitor:         monitor,
		middleware:      middleware,
		shutdownRequest: shutdownRequest,
	}
}

// HandleShutdown orchestrates graceful shutdown based on different shutdown sources
func (h *ShutdownHandler) HandleShutdown(serverDone chan error, osSignals chan os.Signal) error {

	// Goroutine to handle OS signals
	go func() {
		sig, ok := <-osSignals
		if !ok {
			return
		}
		slog.Info("Received OS signal. Initiating shutdown...", "signal", sig)
		h.ShutdownClients(true) // interrupt ongoing processing
	}()

	// Wait for one of three shutdown triggers
	select {
	case err := <-serverDone:
		return h.handleServerError(err)
	case <-h.shutdownRequest:
		return h.handleInternalShutdown(serverDone)
	}
}

// handleServerError handles shutdown when server stops unexpectedly
func (h *ShutdownHandler) handleServerError(err error) error {
	if err != nil {
		slog.Error("Service stopped unexpectedly due to an error", "error", err)
		h.ShutdownClients(true)
		return err
	}
	slog.Info("Service stopped without an error.")
	return nil
}

// handleInternalShutdown handles shutdown triggered by internal scale-in request
func (h *ShutdownHandler) handleInternalShutdown(serverDone chan error) error {
	slog.Info("Received internal scale-in request. Initiating graceful shutdown...")
	h.ShutdownClients(false) // wait for clients to finish
	err := <-serverDone      // wait for server to finish
	if err != nil {
		slog.Error("Service encountered an error during internal shutdown", "error", err)
		return err
	}
	slog.Info("Service exited gracefully after internal scale-in request.")
	return nil
}

// ShutdownClients initiates the shutdown of all server components
func (h *ShutdownHandler) ShutdownClients(interrupt bool) {
	h.logger.Info("Initiating server shutdown...")

	// Stop consuming new messages
	if err := h.middleware.StopConsuming(h.listener.GetConsumerTag()); err != nil {
		h.logger.WithField("error", err).Error("Error stopping consumer")
	}

	// Stop the replica monitor
	h.monitor.Stop()

	if interrupt {
		// Interrupts ongoing processing
		h.listener.InterruptClients(true)
	} else {
		// Waits for clients to finish gracefully
		// if desired, a timeout could be added to set a limit
		// on how long we wait for clients to finish.
		h.listener.InterruptClients(false)
	}

	// Close middleware connection
	h.middleware.Close()

	h.logger.Info("Server stopped consuming, all clients finished.")
}
