package mocks

import (
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/stretchr/testify/mock"
)

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) WithFields(fields map[string]interface{}) processor.Logger {
	args := m.Called(fields)
	if args.Get(0) == nil {
		return m
	}
	return args.Get(0).(processor.Logger)
}

func (m *MockLogger) Info(args ...interface{}) {
	m.Called(args...)
}

func (m *MockLogger) Error(args ...interface{}) {
	m.Called(args...)
}
