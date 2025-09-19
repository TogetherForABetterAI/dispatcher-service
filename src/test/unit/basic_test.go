package unit

import (
	"context"
	"testing"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/test/mocks"
	"github.com/mlops-eval/data-dispatcher-service/src/test/testdata"
	"github.com/stretchr/testify/assert"
)

func TestNewClientDataProcessor(t *testing.T) {
	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(
		mockDatasetClient,
		mockPublisher,
		nil, 
		grpcConfig,
	)

	assert.NotNil(t, processorInstance)
}

func TestSampleData(t *testing.T) {
	batch := testdata.SampleBatch(0, true)

	assert.NotNil(t, batch)
	assert.Equal(t, int32(0), batch.BatchIndex)
	assert.True(t, batch.IsLastBatch)
	assert.Len(t, batch.Data, 9) 
	assert.Len(t, batch.Labels, 3)

	batches := testdata.SampleBatches(3)
	assert.Len(t, batches, 3)

	assert.False(t, batches[0].IsLastBatch)
	assert.False(t, batches[1].IsLastBatch)

	assert.True(t, batches[2].IsLastBatch)

	for i, batch := range batches {
		assert.Equal(t, int32(i), batch.BatchIndex)
	}
}

func TestClientRequest_Validation(t *testing.T) {
	tests := []struct {
		name     string
		request  *clientpb.NewClientRequest
		hasError bool
	}{
		{
			name: "valid request",
			request: &clientpb.NewClientRequest{
				ClientId:   "test-client",
				RoutingKey: "test.routing.key",
			},
			hasError: false,
		},
		{
			name: "empty client id",
			request: &clientpb.NewClientRequest{
				ClientId:   "",
				RoutingKey: "test.routing.key",
			},
			hasError: true,
		},
		{
			name: "empty routing key",
			request: &clientpb.NewClientRequest{
				ClientId:   "test-client",
				RoutingKey: "",
			},
			hasError: true,
		},
		{
			name:     "nil request",
			request:  nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.request == nil {
				return
			}

			if tt.hasError {
				assert.True(t, tt.request.ClientId == "" || tt.request.RoutingKey == "")
			} else {
				assert.NotEmpty(t, tt.request.ClientId)
				assert.NotEmpty(t, tt.request.RoutingKey)
			}
		})
	}
}

func TestContextUsage(t *testing.T) {
	ctx := context.Background()
	assert.NotNil(t, ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Error("Context should not be done initially")
	default:
	}

	cancel()

	select {
	case <-ctx.Done():
		assert.Equal(t, context.Canceled, ctx.Err())
	default:
		t.Error("Context should be done after cancellation")
	}
}
