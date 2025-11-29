package mocks

import (
	"github.com/stretchr/testify/mock"
)

// MockReplicaMonitor is a mock implementation of server.ReplicaMonitorInterface
type MockReplicaMonitor struct {
	mock.Mock
}

func (m *MockReplicaMonitor) Start() {
	m.Called()
}

func (m *MockReplicaMonitor) NotifyWorkerStart() {
	m.Called()
}

func (m *MockReplicaMonitor) NotifyWorkerFinish() {
	m.Called()
}

func (m *MockReplicaMonitor) Stop() {
	m.Called()
}
