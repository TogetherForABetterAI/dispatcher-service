package server

import (
	"testing"

	"github.com/dispatcher-service/src/config"
	"github.com/dispatcher-service/src/middleware"
	"github.com/dispatcher-service/src/mocks"
	"github.com/stretchr/testify/assert"
)

// TestNewListener tests the creation of a new Listener
func TestNewListener(t *testing.T) {
	t.Parallel()
	t.Run("Creates listener with correct fields", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-tag"}
		mockDBClient := new(mocks.MockDBClient)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		// Act
		listener := NewListener(mockMiddleware, cfg, mockDBClient, mockFactory)

		// Assert
		assert.NotNil(t, listener)
		assert.Equal(t, config.CONNECTION_QUEUE_NAME, listener.queueName)
		assert.NotNil(t, listener.logger)
		assert.NotNil(t, listener.jobs)
		assert.NotNil(t, listener.ctx)
		assert.NotNil(t, listener.cancel)
		assert.NotNil(t, listener.clientManagerFactory)
	})
}

// TestListenerGetConsumerTag tests the GetConsumerTag method
func TestListenerGetConsumerTag(t *testing.T) {
	t.Parallel()
	t.Run("Returns correct consumer tag", func(t *testing.T) {
		// Arrange
		mockMiddleware := new(mocks.MockMiddleware)
		cfg := &mocks.MockConfig{WorkerPoolSize: 5, ConsumerTag: "test-consumer-tag"}
		mockDBClient := new(mocks.MockDBClient)
		mockFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) ClientManagerInterface {
			return new(mocks.MockClientManager)
		}

		listener := NewListener(mockMiddleware, cfg, mockDBClient, mockFactory)
		expectedTag := "test-consumer-tag"
		listener.consumerTag = expectedTag

		// Act
		tag := listener.GetConsumerTag()

		// Assert
		assert.Equal(t, expectedTag, tag)
	})
}
