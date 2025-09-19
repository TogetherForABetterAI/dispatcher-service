package mocks

import (
	"context"

	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	"github.com/stretchr/testify/mock"
)

type MockDatasetServiceClient struct {
	mock.Mock
}

func (m *MockDatasetServiceClient) GetBatch(ctx context.Context, req *datasetpb.GetBatchRequest) (*datasetpb.DataBatchLabeled, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*datasetpb.DataBatchLabeled), args.Error(1)
}

func (m *MockDatasetServiceClient) Close() error {
	args := m.Called()
	return args.Error(0)
}
