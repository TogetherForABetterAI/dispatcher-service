package mocks

import (
	"github.com/stretchr/testify/mock"
)

// MockListener is a mock implementation of server.ListenerInterface
type MockListener struct {
	mock.Mock
}

func (m *MockListener) GetConsumerTag() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockListener) InterruptClients() {
	m.Called()
}
