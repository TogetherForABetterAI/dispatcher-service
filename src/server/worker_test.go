package server

import (
	"context"
	"errors"
	"testing"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/mocks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ============================================================================
// TEST HELPERS
// ============================================================================

// createTestWorker creates a worker instance with mocked dependencies for testing
func createTestWorker(factory ClientManagerFactory) (*Worker, *mocks.MockConfig, *mocks.MockMiddleware, *mocks.MockDBClient) {
	mockConfig := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
	mockMiddleware := new(mocks.MockMiddleware)
	mockDBClient := new(mocks.MockDBClient)
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Silence logs in tests

	worker := NewWorker(1, mockConfig, mockMiddleware, mockDBClient, factory, logger)

	return worker, mockConfig, mockMiddleware, mockDBClient
}

// ============================================================================
// TESTS
// ============================================================================

// TestNewWorker tests the worker creation
func TestNewWorker(t *testing.T) {
	t.Parallel()
	t.Run("Creates worker with correct fields", func(t *testing.T) {
		// Arrange
		mockConfig := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
		mockMiddleware := new(mocks.MockMiddleware)
		mockDBClient := new(mocks.MockDBClient)
		logger := logrus.New()
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		// Act
		worker := NewWorker(1, mockConfig, mockMiddleware, mockDBClient, factory, logger)

		// Assert
		assert.NotNil(t, worker)
		assert.Equal(t, 1, worker.id)
		assert.Equal(t, mockConfig, worker.config)
		assert.Equal(t, mockMiddleware, worker.middleware)
		assert.Equal(t, mockDBClient, worker.dbClient)
		assert.NotNil(t, worker.clientManagerFactory)
		assert.NotNil(t, worker.logger)
		assert.Nil(t, worker.clientManager)
		assert.Nil(t, worker.publisher)
	})
}

// TestWorker_processMessage tests the processMessage method with table-driven tests
func TestWorker_processMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		messageBody         string
		mockHandleClientErr error
		expectAck           bool
		expectNack          bool
		expectNackRequeue   bool
		expectFactoryCalled bool
	}{
		{
			name:                "Invalid JSON",
			messageBody:         `{invalid json`,
			mockHandleClientErr: nil,
			expectAck:           false,
			expectNack:          true,
			expectNackRequeue:   false,
			expectFactoryCalled: false,
		},
		{
			name:                "Empty UserID",
			messageBody:         `{"user_id":"","session_id":"session-123","inputs_format":"csv"}`,
			mockHandleClientErr: nil,
			expectAck:           false,
			expectNack:          true,
			expectNackRequeue:   false,
			expectFactoryCalled: false,
		},
		{
			name:                "HandleClient success",
			messageBody:         `{"user_id":"client-123","session_id":"session-123","total_batches_generated":10}`,
			mockHandleClientErr: nil,
			expectAck:           true,
			expectNack:          false,
			expectNackRequeue:   false,
			expectFactoryCalled: true,
		},
		{
			name:                "HandleClient error - requeue",
			messageBody:         `{"user_id":"client-456","session_id":"session-456","total_batches_generated":5}`,
			mockHandleClientErr: errors.New("database error"),
			expectAck:           false,
			expectNack:          true,
			expectNackRequeue:   true,
			expectFactoryCalled: true,
		},
		{
			name:                "HandleClient context.Canceled - no requeue",
			messageBody:         `{"user_id":"client-789","session_id":"session-789","total_batches_generated":3}`,
			mockHandleClientErr: context.Canceled,
			expectAck:           false,
			expectNack:          false,
			expectNackRequeue:   false,
			expectFactoryCalled: true,
		},
		{
			name:                "HandleClient context.DeadlineExceeded - no requeue",
			messageBody:         `{"user_id":"client-999","session_id":"session-999","total_batches_generated":7}`,
			mockHandleClientErr: context.DeadlineExceeded,
			expectAck:           false,
			expectNack:          false,
			expectNackRequeue:   false,
			expectFactoryCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockClientManager := new(mocks.MockClientManager)
			factoryCalled := false
			factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
				factoryCalled = true
				return mockClientManager
			}

			worker, _, _, _ := createTestWorker(factory)

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
			worker.processMessage(mockDelivery.ToDelivery())

			// Assert
			assert.Equal(t, tt.expectFactoryCalled, factoryCalled, "Factory call mismatch")
			mockDelivery.AssertExpectations(t)

			if tt.expectFactoryCalled {
				mockClientManager.AssertExpectations(t)
				assert.Equal(t, mockClientManager, worker.clientManager, "ClientManager should be set")
			}
		})
	}
}

// TestWorker_safeProcessMessage tests the panic recovery mechanism
func TestWorker_safeProcessMessage(t *testing.T) {
	t.Parallel()
	t.Run("Recovers from panic and nacks message", func(t *testing.T) {
		// Arrange
		mockClientManager := new(mocks.MockClientManager)
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return mockClientManager
		}

		worker, _, _, _ := createTestWorker(factory)

		validMsg := `{"user_id":"client-panic","session_id":"session-panic","total_batches_generated":5}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}

		// Mock HandleClient to panic
		mockClientManager.On("HandleClient", mock.Anything).Run(func(args mock.Arguments) {
			panic("BOOM! Something went wrong!")
		}).Once()

		mockDelivery.On("Nack", uint64(1), false, false).Return(nil).Once()

		// Act - this should NOT panic
		assert.NotPanics(t, func() {
			worker.safeProcessMessage(mockDelivery.ToDelivery())
		})

		// Assert
		mockDelivery.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
	})

	t.Run("Processes message normally without panic", func(t *testing.T) {
		// Arrange
		mockClientManager := new(mocks.MockClientManager)
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return mockClientManager
		}

		worker, _, _, _ := createTestWorker(factory)

		validMsg := `{"user_id":"client-normal","session_id":"session-normal","total_batches_generated":3}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}

		// Mock HandleClient to succeed normally
		mockClientManager.On("HandleClient", mock.Anything).Return(nil).Once()
		mockDelivery.On("Ack", uint64(1), false).Return(nil).Once()

		// Act - this should NOT panic
		assert.NotPanics(t, func() {
			worker.safeProcessMessage(mockDelivery.ToDelivery())
		})

		// Assert
		mockDelivery.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
	})
}

// TestWorker_Stop tests the Stop method
func TestWorker_Stop(t *testing.T) {
	t.Parallel()
	t.Run("Calls Stop on active client manager", func(t *testing.T) {
		// Arrange
		mockClientManager := new(mocks.MockClientManager)
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return mockClientManager
		}

		worker, _, _, _ := createTestWorker(factory)
		worker.clientManager = mockClientManager

		mockClientManager.On("Stop").Return().Once()

		// Act
		worker.Stop()

		// Assert
		mockClientManager.AssertExpectations(t)
	})

	t.Run("Does nothing when no active client manager", func(t *testing.T) {
		// Arrange
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		worker, _, _, _ := createTestWorker(factory)
		worker.clientManager = nil

		// Act - should not panic
		assert.NotPanics(t, func() {
			worker.Stop()
		})

		// Assert - just verify it doesn't crash
	})
}

// TestWorker_processMessage_ClientManagerUpdated tests that clientManager is properly set
func TestWorker_processMessage_ClientManagerUpdated(t *testing.T) {
	t.Parallel()
	t.Run("Updates worker's clientManager field", func(t *testing.T) {
		// Arrange
		mockClientManager := new(mocks.MockClientManager)
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return mockClientManager
		}

		worker, _, _, _ := createTestWorker(factory)
		assert.Nil(t, worker.clientManager, "Should start with nil clientManager")

		validMsg := `{"user_id":"client-123","session_id":"session-123","total_batches_generated":5}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}

		mockClientManager.On("HandleClient", mock.Anything).Return(nil).Once()
		mockDelivery.On("Ack", uint64(1), false).Return(nil).Once()

		// Act
		worker.processMessage(mockDelivery.ToDelivery())

		// Assert
		assert.NotNil(t, worker.clientManager, "Should have set clientManager")
		assert.Equal(t, mockClientManager, worker.clientManager)
		mockDelivery.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
	})
}

// TestWorker_processMessage_FactoryReceivesPublisher tests that factory gets publisher
func TestWorker_processMessage_FactoryReceivesPublisher(t *testing.T) {
	t.Parallel()
	t.Run("Factory receives worker's publisher", func(t *testing.T) {
		// Arrange
		mockClientManager := new(mocks.MockClientManager)
		var receivedPublisher *middleware.Publisher
		factory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			receivedPublisher = publisher
			return mockClientManager
		}

		worker, _, _, _ := createTestWorker(factory)

		// Set a mock publisher
		mockPublisher := &middleware.Publisher{}
		worker.publisher = mockPublisher

		validMsg := `{"user_id":"client-123","session_id":"session-123","total_batches_generated":5}`
		mockDelivery := &mocks.MockDelivery{Body: []byte(validMsg)}

		mockClientManager.On("HandleClient", mock.Anything).Return(nil).Once()
		mockDelivery.On("Ack", uint64(1), false).Return(nil).Once()

		// Act
		worker.processMessage(mockDelivery.ToDelivery())

		// Assert
		assert.Equal(t, mockPublisher, receivedPublisher, "Factory should receive worker's publisher")
		mockDelivery.AssertExpectations(t)
		mockClientManager.AssertExpectations(t)
	})
}
