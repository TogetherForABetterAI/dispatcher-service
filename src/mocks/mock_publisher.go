package mocks

import (
	"github.com/stretchr/testify/mock"
)

// MockPublisher is a mock implementation of Publisher for testing
type MockPublisher struct {
	mock.Mock
}

// Publish mocks the Publish method
func (m *MockPublisher) Publish(queueName string, body []byte, routingKey string) error {
	args := m.Called(queueName, body, routingKey)
	return args.Error(0)
}
