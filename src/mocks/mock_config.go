package mocks

import (
	"github.com/data-dispatcher-service/src/config"
)

// MockConfig is a simple implementation of config.Interface for testing
type MockConfig struct {
	WorkerPoolSize int
	ConsumerTag    string
}

func (m *MockConfig) GetLogLevel() string {
	return "info"
}

func (m *MockConfig) GetPodName() string {
	if m.ConsumerTag != "" {
		return m.ConsumerTag
	}
	return "test-pod"
}

func (m *MockConfig) GetMiddlewareConfig() *config.MiddlewareConfig {
	return nil
}

func (m *MockConfig) GetDatabaseConfig() *config.DatabaseConfig {
	return nil
}

func (m *MockConfig) GetWorkerPoolSize() int {
	return m.WorkerPoolSize
}
