package mocks

import (
	"context"

	"github.com/data-dispatcher-service/src/db"
	"github.com/stretchr/testify/mock"
)

// MockDBClient is a mock implementation of db.Client for testing
type MockDBClient struct {
	mock.Mock
}

// GetPendingBatches mocks the GetPendingBatches method
func (m *MockDBClient) GetPendingBatches(ctx context.Context, sessionID string) ([]db.Batch, error) {
	args := m.Called(ctx, sessionID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]db.Batch), args.Error(1)
}

// GetPendingBatchesLimit mocks the GetPendingBatchesLimit method
func (m *MockDBClient) GetPendingBatchesLimit(ctx context.Context, sessionID string, limit int) ([]db.Batch, error) {
	args := m.Called(ctx, sessionID, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]db.Batch), args.Error(1)
}

// MarkBatchAsEnqueued mocks the MarkBatchAsEnqueued method
func (m *MockDBClient) MarkBatchAsEnqueued(ctx context.Context, batchID string) error {
	args := m.Called(ctx, batchID)
	return args.Error(0)
}

// MarkBatchesAsEnqueued mocks the MarkBatchesAsEnqueued method
func (m *MockDBClient) MarkBatchesAsEnqueued(ctx context.Context, sessionIDs []string, batchIndices []int) error {
	args := m.Called(ctx, sessionIDs, batchIndices)
	return args.Error(0)
}

// Close mocks the Close method
func (m *MockDBClient) Close() {
	m.Called()
}
