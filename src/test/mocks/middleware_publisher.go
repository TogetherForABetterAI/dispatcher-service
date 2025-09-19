package mocks

import (
	"github.com/stretchr/testify/mock"
)

type MockMiddlewarePublisher struct {
	mock.Mock
}

func (m *MockMiddlewarePublisher) Publish(routingKey string, message []byte, exchangeName string) error {
	args := m.Called(routingKey, message, exchangeName)
	return args.Error(0)
}

func (m *MockMiddlewarePublisher) Close() {
	m.Called()
}
