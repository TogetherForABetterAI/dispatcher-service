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
	mockListener    *mocks.MockListener
	mockMonitor     *mocks.MockReplicaMonitor
	mockMiddleware  *mocks.MockMiddleware
	logger          *logrus.Logger
	handler         ShutdownHandlerInterface
	serverDone      chan error
	osSignals       chan os.Signal
	shutdownRequest chan struct{}
}

// newShutdownTestHarness creates a new test harness with all mocks initialized
func newShutdownTestHarness(t *testing.T) *shutdownTestHarness {
	t.Helper()

	mockListener := new(mocks.MockListener)
	mockMonitor := new(mocks.MockReplicaMonitor)
	mockMiddleware := new(mocks.MockMiddleware)

	logger := logrus.New()
	logger.SetOutput(logrus.StandardLogger().Out)

	shutdownRequest := make(chan struct{}, 1)

	handler := NewShutdownHandler(
		logger,
		mockListener,
		mockMonitor,
		mockMiddleware,
		shutdownRequest,
	)

	return &shutdownTestHarness{
		mockListener:    mockListener,
		mockMonitor:     mockMonitor,
		mockMiddleware:  mockMiddleware,
		logger:          logger,
		handler:         handler,
		serverDone:      make(chan error, 1),
		osSignals:       make(chan os.Signal, 1),
		shutdownRequest: shutdownRequest,
	}
}

// setupShutdownClientsMocks configures mocks for ShutdownClients with channel-based synchronization
func setupShutdownClientsMocks(h *shutdownTestHarness, interrupt bool, signalChan chan struct{}) {
	h.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
	h.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil)
	h.mockMonitor.On("Stop").Return()

	// Use Run to signal when InterruptClients is called
	h.mockListener.On("InterruptClients", interrupt).Run(func(args mock.Arguments) {
		if signalChan != nil {
			close(signalChan)
		}
	}).Return()

	h.mockMiddleware.On("Close").Return()
}

// ============================================================================
// TESTS
// ============================================================================

// TestShutdownClients tests the ShutdownClients method with different scenarios
func TestShutdownClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interrupt  bool
		setupMocks func(*shutdownTestHarness)
	}{
		{
			name:      "interrupt=false (scale-in graceful)",
			interrupt: false,
			setupMocks: func(h *shutdownTestHarness) {
				h.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
				h.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil)
				h.mockMonitor.On("Stop").Return()
				h.mockListener.On("InterruptClients", false).Return()
				h.mockMiddleware.On("Close").Return()
			},
		},
		{
			name:      "interrupt=true (OS signal forceful)",
			interrupt: true,
			setupMocks: func(h *shutdownTestHarness) {
				h.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
				h.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil)
				h.mockMonitor.On("Stop").Return()
				h.mockListener.On("InterruptClients", true).Return()
				h.mockMiddleware.On("Close").Return()
			},
		},
		{
			name:      "StopConsuming returns error",
			interrupt: false,
			setupMocks: func(h *shutdownTestHarness) {
				h.mockListener.On("GetConsumerTag").Return("test-consumer-tag")
				h.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(fmt.Errorf("consume error"))
				h.mockMonitor.On("Stop").Return()
				h.mockListener.On("InterruptClients", false).Return()
				h.mockMiddleware.On("Close").Return()
			},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			harness := newShutdownTestHarness(t)
			tt.setupMocks(harness)

			// Act
			harness.handler.ShutdownClients(tt.interrupt)

			// Assert
			harness.mockListener.AssertExpectations(t)
			harness.mockMonitor.AssertExpectations(t)
			harness.mockMiddleware.AssertExpectations(t)

			// Verify call order
			harness.mockListener.AssertCalled(t, "GetConsumerTag")
			harness.mockMiddleware.AssertCalled(t, "StopConsuming", "test-consumer-tag")
			harness.mockMonitor.AssertCalled(t, "Stop")
			harness.mockListener.AssertCalled(t, "InterruptClients", tt.interrupt)
			harness.mockMiddleware.AssertCalled(t, "Close")
		})
	}
}

// TestHandleShutdown_InternalShutdown tests graceful scale-in shutdown
func TestHandleShutdown_InternalShutdown(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, false, shutdownDone)

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Trigger internal shutdown (scale-in)
	close(harness.shutdownRequest)

	// Wait for ShutdownClients to be called
	select {
	case <-shutdownDone:
		// ShutdownClients was called
	case <-time.After(1 * time.Second):
		t.Fatal("ShutdownClients was not called within timeout")
	}

	// Simulate listener.Start() finishing cleanly
	harness.serverDone <- nil

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.NoError(t, err, "HandleShutdown should return nil on successful scale-in")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert
	harness.mockListener.AssertCalled(t, "InterruptClients", false)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_OSSignal tests shutdown triggered by OS signal (SIGTERM)
func TestHandleShutdown_OSSignal(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, true, shutdownDone)

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
	harness.mockListener.AssertCalled(t, "InterruptClients", true)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerCrash tests shutdown when server crashes
func TestHandleShutdown_ServerCrash(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, true, shutdownDone)

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

	// Assert - Verify ShutdownClients(true) was called
	harness.mockListener.AssertCalled(t, "InterruptClients", true)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerCrashDuringOSShutdown tests error during OS signal shutdown
func TestHandleShutdown_ServerCrashDuringOSShutdown(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channels to signal when each ShutdownClients call completes
	firstShutdownDone := make(chan struct{})
	secondShutdownDone := make(chan struct{})

	// Setup mocks for first ShutdownClients(true) - called by OS signal goroutine
	harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag").Once()
	harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil).Once()
	harness.mockMonitor.On("Stop").Return().Once()
	harness.mockListener.On("InterruptClients", true).Run(func(args mock.Arguments) {
		close(firstShutdownDone)
	}).Return().Once()
	harness.mockMiddleware.On("Close").Return().Once()

	// Setup mocks for second ShutdownClients(true) - called by handleServerError
	harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag").Once()
	harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil).Once()
	harness.mockMonitor.On("Stop").Return().Once()
	harness.mockListener.On("InterruptClients", true).Run(func(args mock.Arguments) {
		close(secondShutdownDone)
	}).Return().Once()
	harness.mockMiddleware.On("Close").Return().Once()

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Send OS signal
	harness.osSignals <- os.Interrupt

	// Wait for first ShutdownClients to be called
	select {
	case <-firstShutdownDone:
		// First ShutdownClients was called by OS signal goroutine
	case <-time.After(1 * time.Second):
		t.Fatal("First ShutdownClients was not called within timeout")
	}

	// Simulate listener.Start() finishing with error
	// This will trigger handleServerError which calls ShutdownClients again
	crashErr := fmt.Errorf("connection lost")
	harness.serverDone <- crashErr

	// Wait for second ShutdownClients to be called
	select {
	case <-secondShutdownDone:
		// Second ShutdownClients was called by handleServerError
	case <-time.After(1 * time.Second):
		t.Fatal("Second ShutdownClients was not called within timeout")
	}

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.Error(t, err, "HandleShutdown should return error")
		assert.Equal(t, crashErr, err, "HandleShutdown should return the server error")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert - Verify ShutdownClients was called twice with interrupt=true
	harness.mockListener.AssertNumberOfCalls(t, "InterruptClients", 2)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerCrashDuringInternalShutdown tests error during scale-in shutdown
func TestHandleShutdown_ServerCrashDuringInternalShutdown(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to signal when ShutdownClients completes
	shutdownDone := make(chan struct{})
	setupShutdownClientsMocks(harness, false, shutdownDone)

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Trigger internal shutdown (scale-in)
	close(harness.shutdownRequest)

	// Wait for ShutdownClients to be called
	select {
	case <-shutdownDone:
		// ShutdownClients was called
	case <-time.After(1 * time.Second):
		t.Fatal("ShutdownClients was not called within timeout")
	}

	// Simulate listener.Start() finishing with error
	crashErr := fmt.Errorf("database connection failed")
	harness.serverDone <- crashErr

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.Error(t, err, "HandleShutdown should return error")
		assert.Equal(t, crashErr, err, "HandleShutdown should return the server error")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert
	harness.mockListener.AssertCalled(t, "InterruptClients", false)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_Override tests scale-in overridden by OS signal
// This is the most complex scenario where a graceful shutdown is interrupted
func TestHandleShutdown_Override(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// Channel to control when ShutdownClients(false) completes
	gracefulShutdownStarted := make(chan struct{})
	gracefulShutdownBlock := make(chan struct{})

	// Setup mocks for ShutdownClients(false) - this will block
	harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag").Once()
	harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil).Once()
	harness.mockMonitor.On("Stop").Return().Once()

	// This call will block until we close gracefulShutdownBlock
	harness.mockListener.On("InterruptClients", false).Run(func(args mock.Arguments) {
		close(gracefulShutdownStarted) // Signal that graceful shutdown started
		<-gracefulShutdownBlock        // Block here, simulating long wg.Wait()
	}).Return().Once()

	harness.mockMiddleware.On("Close").Return().Once()

	// Setup mocks for ShutdownClients(true) - this will be called by OS signal
	forcefulShutdownDone := make(chan struct{})
	harness.mockListener.On("GetConsumerTag").Return("test-consumer-tag").Once()
	harness.mockMiddleware.On("StopConsuming", "test-consumer-tag").Return(nil).Once()
	harness.mockMonitor.On("Stop").Return().Once()
	harness.mockListener.On("InterruptClients", true).Run(func(args mock.Arguments) {
		close(forcefulShutdownDone)
	}).Return().Once()
	harness.mockMiddleware.On("Close").Return().Once()

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Step 1: Trigger internal shutdown (scale-in)
	close(harness.shutdownRequest)

	// Step 2: Wait for graceful shutdown to start blocking
	select {
	case <-gracefulShutdownStarted:
		// Graceful shutdown has started and is now blocking
	case <-time.After(1 * time.Second):
		t.Fatal("Graceful shutdown did not start within timeout")
	}

	// Step 3: Send OS signal (this should override the graceful shutdown)
	harness.osSignals <- os.Interrupt

	// Step 4: Wait for forceful shutdown to be called
	select {
	case <-forcefulShutdownDone:
		// Forceful shutdown was called
	case <-time.After(1 * time.Second):
		t.Fatal("Forceful shutdown was not called within timeout")
	}

	// Step 5: Unblock graceful shutdown
	close(gracefulShutdownBlock)

	// Step 6: The handleInternalShutdown will now read from serverDone
	harness.serverDone <- nil

	// Step 7: Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.NoError(t, err, "HandleShutdown should complete successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert - Verify both ShutdownClients(false) and ShutdownClients(true) were called
	harness.mockListener.AssertCalled(t, "InterruptClients", false)
	harness.mockListener.AssertCalled(t, "InterruptClients", true)
	harness.mockListener.AssertExpectations(t)
	harness.mockMonitor.AssertExpectations(t)
	harness.mockMiddleware.AssertExpectations(t)
}

// TestHandleShutdown_ServerSuccessfulStop tests when server stops without error
func TestHandleShutdown_ServerSuccessfulStop(t *testing.T) {
	t.Parallel()

	// Arrange
	harness := newShutdownTestHarness(t)

	// No mocks needed for this case as ShutdownClients is not called

	// Channel to receive HandleShutdown result
	resultChan := make(chan error, 1)

	// Act - Launch HandleShutdown in goroutine
	go func() {
		err := harness.handler.HandleShutdown(harness.serverDone, harness.osSignals)
		resultChan <- err
	}()

	// Send nil error to serverDone (server stopped cleanly)
	harness.serverDone <- nil

	// Wait for HandleShutdown to complete
	select {
	case err := <-resultChan:
		assert.NoError(t, err, "HandleShutdown should return nil when server stops cleanly")
	case <-time.After(1 * time.Second):
		t.Fatal("HandleShutdown did not complete within timeout")
	}

	// Assert - Verify no shutdown methods were called
	harness.mockListener.AssertNotCalled(t, "InterruptClients", mock.Anything)
	harness.mockMiddleware.AssertNotCalled(t, "StopConsuming", mock.Anything)
	harness.mockMonitor.AssertNotCalled(t, "Stop")
}
