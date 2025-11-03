package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/mocks"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// TESTS
// ============================================================================

// TestNewListener tests the creation of a new Listener
func TestNewListener(t *testing.T) {
	t.Parallel()
	t.Run("Creates listener with correct fields", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		// Act
		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		// Assert
		assert.NotNil(t, listener)
		assert.Equal(t, config.CONNECTION_QUEUE_NAME, listener.queueName)
		assert.NotNil(t, listener.logger)
		assert.NotNil(t, listener.jobs)
		assert.NotNil(t, listener.ctx)
		assert.NotNil(t, listener.cancel)
		assert.NotNil(t, listener.activeClients)
		assert.NotNil(t, listener.clientManagerFactory)
	})
}

// TestListenerGetConsumerTag tests the GetConsumerTag method
func TestListenerGetConsumerTag(t *testing.T) {
	t.Parallel()
	t.Run("Returns correct consumer tag", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-consumer-tag"}
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)
		expectedTag := "test-consumer-tag"
		listener.consumerTag = expectedTag

		// Act
		tag := listener.GetConsumerTag()

		// Assert
		assert.Equal(t, expectedTag, tag)
	})
}

// TestStart tests the Start method with various scenarios
func TestStart(t *testing.T) {
	t.Parallel()
	t.Run("QoS fails", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		expectedErr := errors.New("QoS error")
		mockMiddleware.On("SetQoS", 5).Return(expectedErr)

		// Act
		err := listener.Start()

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set QoS")
		mockMiddleware.AssertExpectations(t)
	})

	t.Run("BasicConsume fails", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		mockMiddleware.On("SetQoS", 5).Return(nil)
		expectedErr := errors.New("consume error")
		mockMiddleware.On("BasicConsume", config.CONNECTION_QUEUE_NAME, "test-tag").Return(nil, expectedErr)

		// Act
		err := listener.Start()

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to start consuming messages")
		mockMiddleware.AssertExpectations(t)
	})

	t.Run("Success and graceful shutdown", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 2, ConsumerTag: "test-tag"}
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		msgChan := make(chan amqp.Delivery, 1)
		mockMiddleware.On("SetQoS", 2).Return(nil)
		mockMiddleware.On("BasicConsume", config.CONNECTION_QUEUE_NAME, "test-tag").Return((<-chan amqp.Delivery)(msgChan), nil)

		// Act
		errChan := make(chan error, 1)
		go func() {
			errChan <- listener.Start()
		}()

		// Give time for workers to start
		time.Sleep(50 * time.Millisecond)

		// Trigger shutdown
		listener.InterruptClients(false)

		// Wait for Start to return
		err := <-errChan

		// Assert
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		mockMiddleware.AssertExpectations(t)
	})
}

// TestWorker tests the worker lifecycle
func TestWorker(t *testing.T) {
	t.Parallel()
	t.Run("Worker processes message and notifies monitor", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 1, ConsumerTag: "test-tag"}

		mockClientManager := new(mocks.MockClientManager)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return mockClientManager
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		// Set up expectations
		msgChan := make(chan amqp.Delivery, 1)
		mockMiddleware.On("SetQoS", 1).Return(nil)
		mockMiddleware.On("BasicConsume", config.CONNECTION_QUEUE_NAME, "test-tag").Return((<-chan amqp.Delivery)(msgChan), nil)
		mockMonitor.On("NotifyWorkerStart").Return().Once()
		mockMonitor.On("NotifyWorkerFinish").Return().Once()
		mockClientManager.On("HandleClient", mock.Anything).Return(nil).Once()

		// Create a valid message
		validMsg := `{"client_id":"test-client-123","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}
		mockDelivery.On("Ack", uint64(1), false).Return(nil)

		// Act
		go func() {
			listener.Start()
		}()

		// Give time for workers to start
		time.Sleep(50 * time.Millisecond)

		// Send message to jobs channel
		listener.jobs <- mockDelivery.ToDelivery()

		// Close jobs channel to signal workers to finish
		close(listener.jobs)

		// Wait for workers to finish
		listener.wg.Wait()

		// Assert
		mockMonitor.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
		mockDelivery.AssertExpectations(t)
	})
}

// TestSafeProcessMessage tests the panic recovery mechanism
func TestSafeProcessMessage(t *testing.T) {
	t.Parallel()
	t.Run("Recovers from panic and nacks message", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 1, ConsumerTag: "test-tag"}

		mockClientManager := new(mocks.MockClientManager)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return mockClientManager
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		// Create a valid message
		validMsg := `{"client_id":"test-client-456","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}

		// Mock HandleClient to panic
		mockClientManager.On("HandleClient", mock.Anything).Run(func(args mock.Arguments) {
			panic("¡BOOM!")
		})
		mockDelivery.On("Nack", uint64(1), false, false).Return(nil)

		// Act - this should NOT panic
		listener.safeProcessMessage(mockDelivery.ToDelivery())

		// Assert
		mockDelivery.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
	})
}

// TestProcessMessage tests the processMessage method with table-driven tests
func TestProcessMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                       string
		messageBody                string
		mockHandleClientErr        error
		expectAck                  bool
		expectNack                 bool
		expectNackRequeue          bool
		expectFactoryCalled        bool
		expectClientInMap          bool
		expectClientRemovedFromMap bool
	}{
		{
			name:                       "Invalid JSON",
			messageBody:                `{`,
			mockHandleClientErr:        nil,
			expectAck:                  false,
			expectNack:                 true,
			expectNackRequeue:          false,
			expectFactoryCalled:        false,
			expectClientInMap:          false,
			expectClientRemovedFromMap: false,
		},
		{
			name:                       "Empty ClientID",
			messageBody:                `{"client_id":"","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`,
			mockHandleClientErr:        nil,
			expectAck:                  false,
			expectNack:                 true,
			expectNackRequeue:          false,
			expectFactoryCalled:        false,
			expectClientInMap:          false,
			expectClientRemovedFromMap: false,
		},
		{
			name:                       "HandleClient success",
			messageBody:                `{"client_id":"success-client","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`,
			mockHandleClientErr:        nil,
			expectAck:                  true,
			expectNack:                 false,
			expectNackRequeue:          false,
			expectFactoryCalled:        true,
			expectClientInMap:          false, // Should be removed after processing
			expectClientRemovedFromMap: true,
		},
		{
			name:                       "HandleClient transient error",
			messageBody:                `{"client_id":"error-client","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`,
			mockHandleClientErr:        fmt.Errorf("database error"),
			expectAck:                  false,
			expectNack:                 true,
			expectNackRequeue:          true,
			expectFactoryCalled:        true,
			expectClientInMap:          false, // Should be removed after processing
			expectClientRemovedFromMap: true,
		},
		{
			name:                       "HandleClient context canceled",
			messageBody:                `{"client_id":"canceled-client","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`,
			mockHandleClientErr:        context.Canceled,
			expectAck:                  false,
			expectNack:                 false,
			expectNackRequeue:          false,
			expectFactoryCalled:        true,
			expectClientInMap:          false, // Should be removed after processing
			expectClientRemovedFromMap: true,
		},
		{
			name:                       "HandleClient context deadline exceeded",
			messageBody:                `{"client_id":"deadline-client","inputs_format":"csv","outputs_format":"json","model_type":"classification"}`,
			mockHandleClientErr:        context.DeadlineExceeded,
			expectAck:                  false,
			expectNack:                 false,
			expectNackRequeue:          false,
			expectFactoryCalled:        true,
			expectClientInMap:          false, // Should be removed after processing
			expectClientRemovedFromMap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockMiddleware := new(mocks.MockMiddleware)
			mockMonitor := new(mocks.MockReplicaMonitor)
			cfg := &mocks.MockConfig{WorkerPoolSize: 1, ConsumerTag: "test-tag"}

			mockClientManager := new(mocks.MockClientManager)
			factoryCalled := false
			mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
				factoryCalled = true
				return mockClientManager
			}

			listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

			mockDelivery := &mocks.MockDelivery{Body: []byte(tt.messageBody)}

			// Set up expectations based on test case
			if tt.expectFactoryCalled {
				mockClientManager.On("HandleClient", mock.Anything).Return(tt.mockHandleClientErr).Once()
			}

			if tt.expectAck {
				mockDelivery.On("Ack", uint64(1), false).Return(nil).Once()
			}

			if tt.expectNack {
				mockDelivery.On("Nack", uint64(1), false, tt.expectNackRequeue).Return(nil).Once()
			}

			// Act
			listener.processMessage(mockDelivery.ToDelivery())

			// Assert
			assert.Equal(t, tt.expectFactoryCalled, factoryCalled, "Factory call mismatch")
			mockDelivery.AssertExpectations(t)

			if tt.expectFactoryCalled {
				mockClientManager.AssertExpectations(t)
			}

			// Verify client was removed from activeClients map
			listener.clientsMutex.RLock()
			clientCount := len(listener.activeClients)
			listener.clientsMutex.RUnlock()
			assert.Equal(t, 0, clientCount, "activeClients map should be empty after processing")
		})
	}
}

// TestInterruptClients tests the client interruption logic
func TestInterruptClients(t *testing.T) {
	t.Parallel()
	t.Run("interrupt=false does not call Stop on clients", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 1, ConsumerTag: "test-tag"}

		mockClientManager := new(mocks.MockClientManager)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return mockClientManager
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		// Add a client to activeClients
		listener.clientsMutex.Lock()
		listener.activeClients["test-client"] = mockClientManager
		listener.clientsMutex.Unlock()

		// Act
		listener.InterruptClients(false)

		// Assert
		select {
		case <-listener.ctx.Done():
			// Context is cancelled as expected
		default:
			t.Error("Expected context to be cancelled")
		}

		// Stop should NOT have been called
		mockClientManager.AssertNotCalled(t, "Stop")
	})

	t.Run("interrupt=true calls Stop on all active clients", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		mockMonitor := new(mocks.MockReplicaMonitor)
		cfg := &mocks.MockConfig{WorkerPoolSize: 1, ConsumerTag: "test-tag"}

		mockClientManager1 := new(mocks.MockClientManager)
		mockClientManager2 := new(mocks.MockClientManager)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockMonitor, mockFactory)

		// Add clients to activeClients
		listener.clientsMutex.Lock()
		listener.activeClients["test-client-1"] = mockClientManager1
		listener.activeClients["test-client-2"] = mockClientManager2
		listener.clientsMutex.Unlock()

		// Set expectations
		mockClientManager1.On("Stop").Return().Once()
		mockClientManager2.On("Stop").Return().Once()

		// Act
		listener.InterruptClients(true)

		// Assert
		select {
		case <-listener.ctx.Done():
			// Context is cancelled as expected
		default:
			t.Error("Expected context to be cancelled")
		}

		// Stop should have been called on both clients
		mockClientManager1.AssertExpectations(t)
		mockClientManager2.AssertExpectations(t)
	})
}
