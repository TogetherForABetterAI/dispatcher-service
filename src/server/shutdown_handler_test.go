package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/data-dispatcher-service/src/mocks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ============================================================================
// TEST HELPERS
// ============================================================================

// shutdownTestHarness encapsulates all the components needed for shutdown tests
type shutdownTestHarness struct {
	mockListener   *mocks.MockListener
	mockMiddleware *mocks.MockMiddleware
	mockDBClient   *mocks.MockDBClient
	logger         *logrus.Logger
	handler        ShutdownHandlerInterface
	serverDone     chan error
	osSignals      chan os.Signal
}

// newShutdownTestHarness creates a new test harness with all mocks initialized
func newShutdownTestHarness(t *testing.T) *shutdownTestHarness {
	t.Helper()

	mockListener := new(mocks.MockListener)
	mockMiddleware := new(mocks.MockMiddleware)
	mockDBClient := new(mocks.MockDBClient)

	logger := logrus.New()
	logger.SetOutput(logrus.StandardLogger().Out)

	handler := NewShutdownHandler(
		logger,
		mockListener,
		mockMiddleware,
		mockDBClient,
	)

	return &shutdownTestHarness{
		mockListener:   mockListener,
		mockMiddleware: mockMiddleware,
		mockDBClient:   mockDBClient,
		logger:         logger,
		handler:        handler,
		serverDone:     make(chan error, 1),
		osSignals:      make(chan os.Signal, 1),
	}
}

// setupShutdownClientsMocks configures mocks for ShutdownClients with channel-based synchronization
func setupShutdownClientsMocks(h *shutdownTestHarness, signalChan chan struct{}) {
	h.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
	h.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil)

	// Use Run to signal when InterruptClients is called
	h.mockListener.On("InterruptClients").Run(func(args mock.Arguments) {
		if signalChan != nil {
			close(signalChan)
		}
	}).Return()

	h.mockMiddleware.On("Close").Return()
	h.mockDBClient.On("Close").Return()
}

// ============================================================================
// TESTS
// ============================================================================

// TestShutdownClients tests the ShutdownClients method
func TestShutdownClients(t *testing.T) {
	t.Parallel()

	t.Run("interrupts all clients and closes middleware", func(t *testing.T) {
		// Arrange
		harness := newShutdownTestHarness(t)

		harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
		harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil)
		harness.mockListener.On("InterruptClients").Return()
		harness.mockMiddleware.On("Close").Return()
		harness.mockDBClient.On("Close").Return()

		// Act
		harness.handler.ShutdownClients()

		// Assert
		harness.mockListener.AssertExpectations(t)
		harness.mockMiddleware.AssertExpectations(t)
		harness.mockDBClient.AssertExpectations(t)

		// Verify call order
		harness.mockListener.AssertCalled(t, "GetConsumerTag")
		harness.mockMiddleware.AssertCalled(t, "StopConsuming", "test-consumer-tag")
		harness.mockListener.AssertCalled(t, "InterruptClients")
		harness.mockMiddleware.AssertCalled(t, "Close")
		harness.mockDBClient.AssertCalled(t, "Close")
	})

	t.Run("StopConsuming returns error", func(t *testing.T) {
		// Arrange
		harness := newShutdownTestHarness(t)

		harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
		harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(fmt.Errorf("consume error"))
		harness.mockListener.On("InterruptClients").Return()
		harness.mockMiddleware.On("Close").Return()
		harness.mockDBClient.On("Close").Return()

		// Act
		harness.handler.ShutdownClients()

		// Assert
		harness.mockListener.AssertExpectations(t)
		harness.mockMiddleware.AssertExpectations(t)
	})
}

// TestHandleShutdown_OSSignal tests shutdown triggered by OS signal (SIGTERM)
func TestHandleShutdown_OSSignal(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, shutdownDone)

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Send OS signal
	harness.osSignals <- os.Interrupt

	// Wait for ShutdownClients to be called
	select {
	case <-shutdownDone:
		// ShutdownClients was called, now send the serverDone value
		// so the goroutine can continue
		harness.serverDone <- nil
	case <-time.After(1 * time.Second):
		t.Fatal("ShutdownClients was not called within timeout")
	}

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.NoError(t, err, "HandleShutdown should return nil on successful OS signal shutdown")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert
	harness.mockListener.AssertCalled(t, "InterruptClients")
	harness.mockListener.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerError tests shutdown when server stops with error
func TestHandleShutdown_ServerError(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, shutdownDone)

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Send error to serverDone (simulate crash)
	crashErr := fmt.Errorf("BOOM")
	harness.serverDone <- crashErr

	// Wait for ShutdownClients to be called
	select {
	case <-shutdownDone:
		// ShutdownClients was called
	case <-time.After(1 * time.Second):
		t.Fatal("ShutdownClients was not called within timeout")
	}

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.Error(t, err, "HandleShutdown should return error on server crash")
		assert.Equal(t, crashErr, err, "HandleShutdown should return the same error")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert - Verify ShutdownClients was called
	harness.mockListener.AssertCalled(t, "InterruptClients")
	harness.mockListener.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerSuccessfulStop tests when server stops cleanly without error
func TestHandleShutdown_ServerSuccessfulStop(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, shutdownDone)

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Send nil error to serverDone (server stopped cleanly)
	harness.serverDone <- nil

	// Wait for ShutdownClients to be called
	select {
	case <-shutdownDone:
		// ShutdownClients was called
	case <-time.After(1 * time.Second):
		t.Fatal("ShutdownClients was not called within timeout")
	}

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.NoError(t, err, "HandleShutdown should return nil when server stops cleanly")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert - Verify shutdown methods were called
	harness.mockListener.AssertCalled(t, "InterruptClients")
	harness.mockListener.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}
