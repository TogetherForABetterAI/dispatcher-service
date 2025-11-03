package mocks

import (
	"time"

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

func (m *MockConfig) GetReplicaName() string {
	return "test-replica"
}

func (m *MockConfig) GetConsumerTag() string {
	return m.ConsumerTag
}

func (m *MockConfig) GetMiddlewareConfig() *config.MiddlewareConfig {
	return nil
}

func (m *MockConfig) GetGrpcConfig() *config.GrpcConfig {
	return nil
}

func (m *MockConfig) GetWorkerPoolSize() int {
	return m.WorkerPoolSize
}

func (m *MockConfig) IsLeader() bool {
	return false
}

func (m *MockConfig) GetMinThreshold() int {
	return 2
}

func (m *MockConfig) GetStartupTimeout() time.Duration {
	return 5 * time.Second
}
