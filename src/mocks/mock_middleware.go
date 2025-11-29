package mocks

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
)

// MockMiddleware is a mock implementation of middleware.MiddlewareInterface
type MockMiddleware struct {
	mock.Mock
}

func (m *MockMiddleware) SetupTopology() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockMiddleware) DeclareQueue(queueName string) error {
	args := m.Called(queueName)
	return args.Error(0)
}

func (m *MockMiddleware) DeclareExchange(exchangeName string, exchangeType string) error {
	args := m.Called(exchangeName, exchangeType)
	return args.Error(0)
}

func (m *MockMiddleware) BindQueue(queueName, exchangeName, routingKey string) error {
	args := m.Called(queueName, exchangeName, routingKey)
	return args.Error(0)
}

func (m *MockMiddleware) StopConsuming(consumerTag string) error {
	args := m.Called(consumerTag)
	return args.Error(0)
}

func (m *MockMiddleware) SetQoS(prefetchCount int) error {
	args := m.Called(prefetchCount)
	return args.Error(0)
}

func (m *MockMiddleware) BasicConsume(queueName string, consumerTag string) (<-chan amqp.Delivery, error) {
	args := m.Called(queueName, consumerTag)
	if ch := args.Get(0); ch != nil {
		return ch.(<-chan amqp.Delivery), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockMiddleware) Close() {
	m.Called()
}

func (m *MockMiddleware) Conn() *amqp.Connection {
	args := m.Called()
	if conn := args.Get(0); conn != nil {
		return conn.(*amqp.Connection)
	}
	return nil
}
