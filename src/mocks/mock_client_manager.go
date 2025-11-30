package mocks

import (
	"github.com/dispatcher-service/src/models"
	"github.com/stretchr/testify/mock"
)

// MockClientManager is a mock implementation of server.ClientManagerInterface
type MockClientManager struct {
	mock.Mock
}

func (m *MockClientManager) HandleClient(notification *models.ConnectNotification) error {
	args := m.Called(notification)
	return args.Error(0)
}

func (m *MockClientManager) Stop() {
	m.Called()
}
