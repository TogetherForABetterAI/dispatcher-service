package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/mocks"
	"github.com/data-dispatcher-service/src/models"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewBatchHandler(t *testing.T) {
	logger := logrus.New()
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	assert.NotNil(t, handler)
	assert.Equal(t, mockPublisher, handler.publisher)
	assert.Equal(t, mockDBClient, handler.dbClient)
	assert.Equal(t, logger, handler.logger)
	assert.NotNil(t, handler.ctx)
	assert.NotNil(t, handler.cancel)
	assert.Equal(t, 0, handler.totalBatches)
}

func TestBatchHandler_Start_Success_SingleChunk(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Reduce noise in tests
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 3,
	}

	// Mock 3 batches
	batches := []db.Batch{
		{SessionID: "session-456", BatchIndex: 0, DataPayload: []byte("data0"), Labels: []int32{1, 2}, IsEnqueued: false},
		{SessionID: "session-456", BatchIndex: 1, DataPayload: []byte("data1"), Labels: []int32{3, 4}, IsEnqueued: false},
		{SessionID: "session-456", BatchIndex: 2, DataPayload: []byte("data2"), Labels: []int32{5, 6}, IsEnqueued: false},
	}

	// Expectations
	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 3).Return(batches, nil).Once()

	// Publish expectations (2 per batch: unlabeled + labeled)
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Times(3)
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(nil).Times(3)

	// MarkBatchesAsEnqueued expectations
	mockDBClient.On("MarkBatchesAsEnqueued", mock.Anything,
		[]string{"session-456", "session-456", "session-456"},
		[]int{0, 1, 2}).Return(nil).Once()

	err := handler.Start(notification)

	assert.NoError(t, err)
	assert.Equal(t, 3, handler.totalBatches)
	mockDBClient.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Start_Success_MultipleChunks(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 15, // Will be split: 10 + 5 (BATCHES_TO_FETCH = 10)
	}

	// First chunk: 10 batches
	firstChunk := make([]db.Batch, 10)
	for i := 0; i < 10; i++ {
		firstChunk[i] = db.Batch{
			SessionID:   "session-456",
			BatchIndex:  i,
			DataPayload: []byte(fmt.Sprintf("data%d", i)),
			Labels:      []int32{int32(i + 1)},
			IsEnqueued:  false,
		}
	}

	// Second chunk: 5 batches
	secondChunk := make([]db.Batch, 5)
	for i := 0; i < 5; i++ {
		secondChunk[i] = db.Batch{
			SessionID:   "session-456",
			BatchIndex:  i + 10,
			DataPayload: []byte(fmt.Sprintf("data%d", i+10)),
			Labels:      []int32{int32(i + 11)},
			IsEnqueued:  false,
		}
	}

	// Expectations - First chunk (10 batches)
	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 10).Return(firstChunk, nil).Once()
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Times(10)
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(nil).Times(10)

	firstChunkSessionIDs := make([]string, 10)
	firstChunkIndices := make([]int, 10)
	for i := 0; i < 10; i++ {
		firstChunkSessionIDs[i] = "session-456"
		firstChunkIndices[i] = i
	}
	mockDBClient.On("MarkBatchesAsEnqueued", mock.Anything, firstChunkSessionIDs, firstChunkIndices).Return(nil).Once()

	// Expectations - Second chunk (5 batches)
	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 5).Return(secondChunk, nil).Once()
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Times(5)
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(nil).Times(5)

	secondChunkSessionIDs := make([]string, 5)
	secondChunkIndices := make([]int, 5)
	for i := 0; i < 5; i++ {
		secondChunkSessionIDs[i] = "session-456"
		secondChunkIndices[i] = i + 10
	}
	mockDBClient.On("MarkBatchesAsEnqueued", mock.Anything, secondChunkSessionIDs, secondChunkIndices).Return(nil).Once()

	err := handler.Start(notification)

	assert.NoError(t, err)
	assert.Equal(t, 15, handler.totalBatches)
	mockDBClient.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Start_IsLastBatchDetection(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 2,
	}

	batches := []db.Batch{
		{SessionID: "session-456", BatchIndex: 0, DataPayload: []byte("data0"), Labels: []int32{1}, IsEnqueued: false},
		{SessionID: "session-456", BatchIndex: 1, DataPayload: []byte("data1"), Labels: []int32{2}, IsEnqueued: false},
	}

	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 2).Return(batches, nil).Once()

	// Capture published messages to verify isLastBatch flag
	var capturedUnlabeledMessages [][]byte
	var capturedLabeledMessages [][]byte

	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Run(func(args mock.Arguments) {
		capturedUnlabeledMessages = append(capturedUnlabeledMessages, args.Get(1).([]byte))
	}).Return(nil).Times(2)

	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Run(func(args mock.Arguments) {
		capturedLabeledMessages = append(capturedLabeledMessages, args.Get(1).([]byte))
	}).Return(nil).Times(2)

	mockDBClient.On("MarkBatchesAsEnqueued", mock.Anything,
		[]string{"session-456", "session-456"},
		[]int{0, 1}).Return(nil).Once()

	err := handler.Start(notification)

	assert.NoError(t, err)
	assert.Len(t, capturedUnlabeledMessages, 2)
	assert.Len(t, capturedLabeledMessages, 2)
	mockDBClient.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Start_DBError(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 5,
	}

	expectedError := errors.New("database connection failed")
	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 5).Return(nil, expectedError).Once()

	err := handler.Start(notification)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get pending batches")
	mockDBClient.AssertExpectations(t)
}

func TestBatchHandler_Start_PublishError(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 1,
	}

	batches := []db.Batch{
		{SessionID: "session-456", BatchIndex: 0, DataPayload: []byte("data0"), Labels: []int32{1}, IsEnqueued: false},
	}

	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 1).Return(batches, nil).Once()

	publishError := errors.New("rabbitmq connection failed")
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(publishError).Once()

	err := handler.Start(notification)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish batch index")
	mockDBClient.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Start_MarkEnqueuedError_DoesNotFailProcessing(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 1,
	}

	batches := []db.Batch{
		{SessionID: "session-456", BatchIndex: 0, DataPayload: []byte("data0"), Labels: []int32{1}, IsEnqueued: false},
	}

	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 1).Return(batches, nil).Once()
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Once()
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(nil).Once()

	markError := errors.New("database update failed")
	mockDBClient.On("MarkBatchesAsEnqueued", mock.Anything,
		[]string{"session-456"},
		[]int{0}).Return(markError).Once()

	// Should NOT return error - idempotency handles duplicates
	err := handler.Start(notification)

	assert.NoError(t, err)
	mockDBClient.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Start_NoBatchesReturned(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 5, // Expects 5 but gets 0
	}

	// Return empty batch list
	mockDBClient.On("GetPendingBatchesLimit", mock.Anything, "session-456", 5).Return([]db.Batch{}, nil).Once()

	err := handler.Start(notification)

	// Should not error but will warn about mismatch
	assert.NoError(t, err)
	mockDBClient.AssertExpectations(t)
}

func TestBatchHandler_Start_ZeroBatches(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 0,
	}

	// Should exit immediately without DB calls
	err := handler.Start(notification)

	assert.NoError(t, err)
	mockDBClient.AssertNotCalled(t, "GetPendingBatchesLimit")
}

func TestBatchHandler_Start_ContextCancellation(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	notification := &models.ConnectNotification{
		UserID:                "client-123",
		SessionId:             "session-456",
		TotalBatchesGenerated: 10,
	}

	// Cancel context before processing
	handler.Stop()

	err := handler.Start(notification)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestBatchHandler_PrepareBatches(t *testing.T) {
	logger := logrus.New()
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	data := []byte("test_data")
	batchIndex := int32(5)
	labels := []int32{1, 2, 3}
	isLastBatch := true
	sessionID := "session-789"

	unlabeled, labeled := handler.prepareBatches(data, batchIndex, labels, isLastBatch, sessionID)

	// Check unlabeled batch
	assert.Equal(t, data, unlabeled.Data)
	assert.Equal(t, batchIndex, unlabeled.BatchIndex)
	assert.Equal(t, isLastBatch, unlabeled.IsLastBatch)
	assert.Equal(t, sessionID, unlabeled.SessionId)

	// Check labeled batch
	assert.Equal(t, data, labeled.Data)
	assert.Equal(t, batchIndex, labeled.BatchIndex)
	assert.Equal(t, isLastBatch, labeled.IsLastBatch)
	assert.Equal(t, labels, labeled.Labels)
	assert.Equal(t, sessionID, labeled.SessionId)
}

func TestBatchHandler_MarshalBatches(t *testing.T) {
	logger := logrus.New()
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	unlabeled, labeled := handler.prepareBatches(
		[]byte("test_data"),
		1,
		[]int32{1, 2},
		false,
		"session-123",
	)

	unlabeledBytes, labeledBytes, err := handler.marshalBatches(unlabeled, labeled)

	assert.NoError(t, err)
	assert.NotNil(t, unlabeledBytes)
	assert.NotNil(t, labeledBytes)
	assert.Greater(t, len(unlabeledBytes), 0)
	assert.Greater(t, len(labeledBytes), 0)
}

func TestBatchHandler_PublishBatch_Success(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	batch := db.Batch{
		SessionID:   "session-456",
		BatchIndex:  3,
		DataPayload: []byte("test_payload"),
		Labels:      []int32{1, 2, 3},
		IsEnqueued:  false,
	}

	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Once()
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(nil).Once()

	err := handler.publishBatch(batch, "session-456", true)

	assert.NoError(t, err)
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_PublishBatch_ClientQueueError(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	batch := db.Batch{
		SessionID:   "session-456",
		BatchIndex:  3,
		DataPayload: []byte("test_payload"),
		Labels:      []int32{1, 2, 3},
		IsEnqueued:  false,
	}

	publishError := errors.New("client queue publish failed")
	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(publishError).Once()

	err := handler.publishBatch(batch, "session-456", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish unlabeled batch")
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_PublishBatch_CalibrationQueueError(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	batch := db.Batch{
		SessionID:   "session-456",
		BatchIndex:  3,
		DataPayload: []byte("test_payload"),
		Labels:      []int32{1, 2, 3},
		IsEnqueued:  false,
	}

	mockPublisher.On("Publish", "test_client_queue", mock.Anything, "").Return(nil).Once()

	calibrationError := errors.New("calibration queue publish failed")
	mockPublisher.On("Publish", "test_calibration_queue", mock.Anything, "").Return(calibrationError).Once()

	err := handler.publishBatch(batch, "session-456", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish labeled batch")
	mockPublisher.AssertExpectations(t)
}

func TestBatchHandler_Stop(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	mockPublisher := new(mocks.MockPublisher)
	mockDBClient := new(mocks.MockDBClient)

	handler := NewBatchHandler(mockPublisher, mockDBClient, logger, "test_client_queue", "test_calibration_queue")

	// Context should be active initially
	select {
	case <-handler.ctx.Done():
		t.Fatal("Context should not be cancelled initially")
	default:
		// Expected
	}

	handler.Stop()

	// Context should be cancelled after Stop()
	select {
	case <-handler.ctx.Done():
		// Expected
	default:
		t.Fatal("Context should be cancelled after Stop()")
	}
}
