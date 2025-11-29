package server

import (
	"context"
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/models"
	"github.com/data-dispatcher-service/src/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// PublisherInterface defines the interface for publishing messages
type PublisherInterface interface {
	Publish(queueName string, body []byte, routingKey string) error
}

// BatchHandler manages fetching batches from DB and publishing to client queues
type BatchHandler struct {
	publisher                    PublisherInterface
	dbClient                     DBClient
	logger                       *logrus.Logger
	ctx                          context.Context
	cancel                       context.CancelFunc
	dispatcherToClientQueue      string
	dispatcherToCalibrationQueue string
	totalBatches                 int // Total batches generated for this session
}

// NewBatchHandler creates a new batch handler with initialized dependencies
func NewBatchHandler(publisher PublisherInterface, dbClient DBClient, logger *logrus.Logger, dispatcherToClientQueue, dispatcherToCalibrationQueue string) *BatchHandler {
	ctx, cancel := context.WithCancel(context.Background())

	return &BatchHandler{
		publisher:                    publisher,
		dbClient:                     dbClient,
		logger:                       logger,
		ctx:                          ctx,
		cancel:                       cancel,
		dispatcherToClientQueue:      dispatcherToClientQueue,
		dispatcherToCalibrationQueue: dispatcherToCalibrationQueue,
		totalBatches:                 0,
	}
}

// Start processes all batches for a client session in chunks
func (bh *BatchHandler) Start(notification *models.ConnectNotification) error {
	// Store total batches for this session
	bh.totalBatches = notification.TotalBatchesGenerated

	bh.logger.WithFields(logrus.Fields{
		"user_id":          notification.UserID,
		"session_id":       notification.SessionId,
		"total_batches":    bh.totalBatches,
		"dataset_exchange": config.DATASET_EXCHANGE,
	}).Info("Starting batch handler for client session")

	totalProcessed := 0

	// Loop until no more pending batches
	for {
		select {
		case <-bh.ctx.Done():
			return bh.ctx.Err() // Context cancelled due to shutdown signal
		default:
			// Continue processing
		}

		// Calculate how many batches remain
		batchesRemaining := bh.totalBatches - totalProcessed

		if batchesRemaining <= 0 {
			bh.logger.WithFields(logrus.Fields{
				"user_id":       notification.UserID,
				"session_id":    notification.SessionId,
				"total_batches": bh.totalBatches,
			}).Info("All batches processed for client session")
			break // All batches processed
		}

		// Get the next chunk of batches
		batches, err := bh.dbClient.GetPendingBatchesLimit(
			bh.ctx,
			notification.SessionId,
			min(config.BATCHES_TO_FETCH, batchesRemaining),
		)
		if err != nil {
			return fmt.Errorf("failed to get pending batches: %w", err)
		}

		// If no batches returned but we expected some, something is wrong
		if len(batches) == 0 {
			bh.logger.WithFields(logrus.Fields{
				"user_id":            notification.UserID,
				"session_id":         notification.SessionId,
				"total_processed":    totalProcessed,
				"expected_remaining": batchesRemaining,
			}).Warn("No batches returned but expected more based on total_batches")
			break
		}

		// Determine if this is the last chunk
		isLastChunk := (totalProcessed + len(batches)) >= bh.totalBatches

		bh.logger.WithFields(logrus.Fields{
			"session_id":        notification.SessionId,
			"chunk_size":        len(batches),
			"is_last_chunk":     isLastChunk,
			"batches_processed": totalProcessed,
			"batches_remaining": batchesRemaining,
			"progress_percent":  float64(totalProcessed) / float64(bh.totalBatches) * 100,
		}).Debug("Retrieved chunk of pending batches")

		// Process this chunk
		if err := bh.processBatchChunk(batches, notification, isLastChunk); err != nil {
			return fmt.Errorf("failed to process batch chunk: %w", err)
		}

		totalProcessed += len(batches)

		// If this was the last chunk, we're done
		if isLastChunk {
			break
		}
	}

	return nil
}

// processBatchChunk handles publishing and marking a chunk of batches
func (bh *BatchHandler) processBatchChunk(batches []db.Batch, notification *models.ConnectNotification, isLastChunk bool) error {
	sessionIDs := make([]string, 0, len(batches))
	batchIndices := make([]int, 0, len(batches))

	// Publish all batches in the chunk
	for i, batch := range batches {
		select {
		case <-bh.ctx.Done():
			return bh.ctx.Err()
		default:
			// Continue processing
		}

		// Determine if this is the last batch of the entire session
		// It's the last batch ONLY if:
		// 1. This is the last chunk (isLastChunk == true) AND
		// 2. This is the last item in this chunk (i == len(batches) - 1)
		isLastBatch := isLastChunk && (i == len(batches)-1)

		// Publish the batch to both destinations
		if err := bh.publishBatch(batch, notification.SessionId, isLastBatch); err != nil {
			// If publish fails, don't mark any batch as enqueued
			return fmt.Errorf("failed to publish batch index %d (position %d in chunk): %w", batch.BatchIndex, i, err)
		}

		sessionIDs = append(sessionIDs, batch.SessionID)
		batchIndices = append(batchIndices, batch.BatchIndex)

		bh.logger.WithFields(logrus.Fields{
			"user_id":       notification.UserID,
			"session_id":    notification.SessionId,
			"batch_index":   batch.BatchIndex,
			"total_batches": bh.totalBatches,
			"is_last_batch": isLastBatch,
		}).Info("Batch published successfully")
	}

	// Mark all batches in this chunk as enqueued in a single DB operation
	if err := bh.dbClient.MarkBatchesAsEnqueued(bh.ctx, sessionIDs, batchIndices); err != nil {
		bh.logger.WithError(err).WithFields(logrus.Fields{
			"batch_count":   len(batchIndices),
			"batch_indices": batchIndices,
		}).Error("Failed to mark batches as enqueued, but messages were published. Idempotency will handle duplicates.")
		// Don't return error - messages already published, idempotency will handle it
	}

	bh.logger.WithFields(logrus.Fields{
		"user_id":       notification.UserID,
		"session_id":    notification.SessionId,
		"batch_count":   len(batches),
		"total_batches": bh.totalBatches,
		"is_last_chunk": isLastChunk,
	}).Info("Successfully processed batch chunk")

	return nil
}

// publishBatch handles the transformation and publishing of a single batch to both destinations
func (bh *BatchHandler) publishBatch(batch db.Batch, sessionID string, isLastBatch bool) error {
	// Prepare both batch types (with and without labels)
	unlabeledBatch, labeledBatch := bh.prepareBatches(
		batch.DataPayload,
		int32(batch.BatchIndex),
		batch.Labels,
		isLastBatch,
		sessionID,
	)

	// Marshal batches to protobuf
	unlabeledBody, labeledBody, err := bh.marshalBatches(unlabeledBatch, labeledBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal batches: %w", err)
	}

	if err := bh.publisher.Publish(bh.dispatcherToClientQueue, unlabeledBody, ""); err != nil {
		return fmt.Errorf("failed to publish unlabeled batch to client queue %s: %w", bh.dispatcherToClientQueue, err)
	}

	if err := bh.publisher.Publish(bh.dispatcherToCalibrationQueue, labeledBody, ""); err != nil {
		return fmt.Errorf("failed to publish labeled batch to dataset exchange: %w", err)
	}

	bh.logger.WithFields(logrus.Fields{
		"session_id":     sessionID,
		"batch_index":    batch.BatchIndex,
		"client_queue":   bh.dispatcherToClientQueue,
		"internal_queue": bh.dispatcherToCalibrationQueue,
		"is_last_batch":  isLastBatch,
	}).Info("Batch published to both destinations")

	return nil
}

// prepareBatches creates the unlabeled and labeled protobuf batches
func (bh *BatchHandler) prepareBatches(data []byte, batchIndex int32, labels []int32, isLastBatch bool, sessionID string) (*pb.DataBatchUnlabeled, *pb.DataBatchLabeled) {
	unlabeledBatch := &pb.DataBatchUnlabeled{
		Data:        data,
		BatchIndex:  batchIndex,
		IsLastBatch: isLastBatch,
		SessionId:   sessionID,
	}

	labeledBatch := &pb.DataBatchLabeled{
		Data:        data,
		BatchIndex:  batchIndex,
		IsLastBatch: isLastBatch,
		Labels:      labels,
		SessionId:   sessionID,
	}

	return unlabeledBatch, labeledBatch
}

// marshalBatches serializes the batches to protobuf
func (bh *BatchHandler) marshalBatches(unlabeledBatch *pb.DataBatchUnlabeled, labeledBatch *pb.DataBatchLabeled) ([]byte, []byte, error) {
	unlabeledBody, err := proto.Marshal(unlabeledBatch)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal unlabeled batch: %w", err)
	}

	labeledBody, err := proto.Marshal(labeledBatch)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal labeled batch: %w", err)
	}

	return unlabeledBody, labeledBody, nil
}

// Stop cancels the batch handler's context
func (bh *BatchHandler) Stop() {
	bh.cancel()
	bh.logger.Info("BatchHandler stopped")
}
