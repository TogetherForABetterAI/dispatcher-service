package mocks

import (
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/stretchr/testify/mock"
)

type MockConfigProvider struct {
	mock.Mock
}

func (m *MockConfigProvider) GetDatasetServiceAddr() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConfigProvider) GetMaxRetries() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockConfigProvider) GetDatasetName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConfigProvider) GetBatchSize() int32 {
	args := m.Called()
	return args.Get(0).(int32)
}

func (m *MockConfigProvider) GetRabbitConfig() *config.MiddlewareConfig {
	args := m.Called()
	return args.Get(0).(*config.MiddlewareConfig)
}
