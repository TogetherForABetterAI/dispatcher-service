package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/test/mocks"
	"github.com/mlops-eval/data-dispatcher-service/src/test/testdata"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

func TestClientDataProcessor_ProcessClient_SingleBatch(t *testing.T) {
	ctx := context.Background()

	mockMiddleware := &mocks.MockMiddlewarePublisher{}
	mockDatasetClient := &mocks.MockDatasetServiceClient{}

	batch := testdata.SampleBatch(0, true) 

	mockDatasetClient.On("GetBatch", mock.Anything, mock.Anything).Return(batch, nil).Once()

	mockMiddleware.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockMiddleware.On("Close").Return()

	processorInstance := CreateTestProcessor(mockDatasetClient, mockMiddleware)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.NoError(t, err)
	mockDatasetClient.AssertNumberOfCalls(t, "GetBatch", 1)
	mockMiddleware.AssertNumberOfCalls(t, "Publish", 2)
}

func TestClientDataProcessor_ProcessClient_Success(t *testing.T) {
	ctx := context.Background()

	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}
	logger := logrus.New()

	batch1 := testdata.SampleBatch(0, false) 
	batch2 := testdata.SampleBatch(1, true)

	mockDatasetClient.On("GetBatch", mock.Anything, mock.MatchedBy(func(req *datasetpb.GetBatchRequest) bool {
		return req.BatchIndex == 0 && req.DatasetName == "mnist" && req.BatchSize == 300
	})).Return(batch1, nil).Once()

	mockDatasetClient.On("GetBatch", mock.Anything, mock.MatchedBy(func(req *datasetpb.GetBatchRequest) bool {
		return req.BatchIndex == 1 && req.DatasetName == "mnist" && req.BatchSize == 300
	})).Return(batch2, nil).Once()

	mockPublisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(4)

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(mockDatasetClient, mockPublisher, logger, grpcConfig)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.NoError(t, err)

	mockDatasetClient.AssertNumberOfCalls(t, "GetBatch", 2)
	mockPublisher.AssertNumberOfCalls(t, "Publish", 4)
}

func TestClientDataProcessor_ProcessClient_DatasetServiceError(t *testing.T) {
	ctx := context.Background()

	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}
	logger := logrus.New()

	mockDatasetClient.On("GetBatch", mock.Anything, mock.Anything).Return(
		(*datasetpb.DataBatchLabeled)(nil), errors.New("dataset service unavailable"))

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(mockDatasetClient, mockPublisher, logger, grpcConfig)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch batch 0")
	assert.Contains(t, err.Error(), "dataset service unavailable")
}

func TestClientDataProcessor_ProcessClient_PublishError(t *testing.T) {
	ctx := context.Background()

	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}
	logger := logrus.New()

	batch := testdata.SampleBatch(0, true)
	mockDatasetClient.On("GetBatch", mock.Anything, mock.Anything).Return(batch, nil)

	mockPublisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(
		errors.New("rabbitmq publish failed"))

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(mockDatasetClient, mockPublisher, logger, grpcConfig)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish batch 0")
	assert.Contains(t, err.Error(), "rabbitmq publish failed")
}

func TestClientDataProcessor_ProcessClient_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}
	logger := logrus.New()

	batch1 := testdata.SampleBatch(0, false)
	batch2 := testdata.SampleBatch(1, true)

	mockDatasetClient.On("GetBatch", mock.Anything, mock.MatchedBy(func(req *datasetpb.GetBatchRequest) bool {
		return req.BatchIndex == 0
	})).Return(batch1, nil)

	mockDatasetClient.On("GetBatch", mock.Anything, mock.MatchedBy(func(req *datasetpb.GetBatchRequest) bool {
		return req.BatchIndex == 1
	})).Return(batch2, nil)

	mockPublisher.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(mockDatasetClient, mockPublisher, logger, grpcConfig)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.Error(t, err)
	assert.True(t,
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
		"Expected context cancellation error, got: %v", err)
}

func TestClientDataProcessor_ProcessClient_MessageSerialization(t *testing.T) {
	ctx := context.Background()

	mockDatasetClient := &mocks.MockDatasetServiceClient{}
	mockPublisher := &mocks.MockMiddlewarePublisher{}
	logger := logrus.New()

	batch := testdata.SampleBatch(0, true)
	mockDatasetClient.On("GetBatch", mock.Anything, mock.Anything).Return(batch, nil)

	var publishedMessages []struct {
		routingKey   string
		body         []byte
		exchangeName string
	}

	mockPublisher.On("Publish", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8"), mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) {
			publishedMessages = append(publishedMessages, struct {
				routingKey   string
				body         []byte
				exchangeName string
			}{
				routingKey:   args.String(0),
				body:         args.Get(1).([]byte),
				exchangeName: args.String(2),
			})
		}).Return(nil)

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	processorInstance := processor.NewClientDataProcessor(mockDatasetClient, mockPublisher, logger, grpcConfig)

	req := &clientpb.NewClientRequest{
		ClientId:   "test-client-123",
		RoutingKey: "test.routing.key",
	}

	err := processorInstance.ProcessClient(ctx, req)

	assert.NoError(t, err)
	assert.Len(t, publishedMessages, 2)
	assert.Equal(t, "test.routing.key.unlabeled", publishedMessages[0].routingKey)
	assert.Equal(t, "test.routing.key.labeled", publishedMessages[1].routingKey)

	var unlabeledBatch datasetpb.DataBatchUnlabeled
	err = proto.Unmarshal(publishedMessages[0].body, &unlabeledBatch)
	assert.NoError(t, err)
	assert.Equal(t, batch.BatchIndex, unlabeledBatch.BatchIndex)
	assert.Equal(t, batch.IsLastBatch, unlabeledBatch.IsLastBatch)
	assert.Equal(t, batch.Data, unlabeledBatch.Data)
	
	var labeledBatch datasetpb.DataBatchLabeled
	err = proto.Unmarshal(publishedMessages[1].body, &labeledBatch)
	assert.NoError(t, err)
	assert.Equal(t, batch.BatchIndex, labeledBatch.BatchIndex)
	assert.Equal(t, batch.IsLastBatch, labeledBatch.IsLastBatch)
	assert.Equal(t, batch.Data, labeledBatch.Data)
	assert.Equal(t, batch.Labels, labeledBatch.Labels)
}
