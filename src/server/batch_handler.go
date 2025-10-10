package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/mlops-eval/data-dispatcher-service/src/models"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// BatchHandler manages middleware and gRPC client instances for batch processing
type BatchHandler struct {
	middleware *middleware.Middleware
	grpcClient *grpc.Client
	logger     *logrus.Logger
	modelType  string
	batchSize  int32
}

// NewBatchHandler creates a new batch handler with initialized dependencies
func NewBatchHandler(middlewareInstance *middleware.Middleware, grpcClient *grpc.Client, modelType string, batchSize int32, logger *logrus.Logger) *BatchHandler {
	return &BatchHandler{
		middleware: middlewareInstance,
		grpcClient: grpcClient,
		logger:     logger,
		modelType:  modelType,
		batchSize:  batchSize,
	}
}

// Start initializes the batch handler and processes all batches for the client
func (bh *BatchHandler) Start(ctx context.Context, notification *models.ConnectNotification) error {
	bh.logger.WithFields(logrus.Fields{
		"client_id":  notification.ClientId,
		"model_type": notification.ModelType,
		"batch_size": bh.batchSize,
	}).Info("Starting batch handler and processing client data")

	// Process all batches for this client
	return bh.processBatches(ctx, notification)
}

// processBatches handles the main batch processing loop
func (bh *BatchHandler) processBatches(ctx context.Context, notification *models.ConnectNotification) error {
	batchIndex := int32(0)

	for {
		// Fetch batch from dataset service using batch handler
		batch, err := bh.FetchBatch(ctx, batchIndex)
		if err != nil {
			return fmt.Errorf("failed to fetch batch %d: %w", batchIndex, err)
		}

		// Publish the batch using batch handler
		if err := bh.PublishBatch(notification, batch); err != nil {
			return fmt.Errorf("failed to process batch %d: %w", batchIndex, err)
		}

		bh.logger.WithFields(logrus.Fields{
			"client_id":     notification.ClientId,
			"model_type":    notification.ModelType,
			"batch_index":   batchIndex,
			"is_last_batch": batch.GetIsLastBatch(),
			"data_size":     len(batch.GetData()),
		}).Info("Successfully published batch to both exchanges")

		// Check if this was the last batch
		if batch.GetIsLastBatch() {
			bh.logger.WithFields(logrus.Fields{
				"client_id":     notification.ClientId,
				"model_type":    notification.ModelType,
				"total_batches": batchIndex + 1,
			}).Info("Completed data processing for client")
			break
		}

		batchIndex++

		// Add small delay between batches to avoid overwhelming the services
		if err := bh.waitBetweenBatches(ctx); err != nil {
			return err
		}
	}

	return nil
}

// waitBetweenBatches adds a small delay between batch processing
func (bh *BatchHandler) waitBetweenBatches(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

// FetchBatch retrieves a single batch from the dataset service
func (bh *BatchHandler) FetchBatch(ctx context.Context, batchIndex int32) (*datasetpb.DataBatchLabeled, error) {
	batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	batchReq := &datasetpb.GetBatchRequest{
		ModelType:  bh.modelType,
		BatchSize:  bh.batchSize,
		BatchIndex: batchIndex,
	}

	batch, err := bh.grpcClient.GetBatch(batchCtx, batchReq)
	if err != nil {
		bh.logger.WithFields(logrus.Fields{
			"batch_index": batchIndex,
			"error":       err.Error(),
		}).Error("Failed to fetch batch from dataset service")
		return nil, err
	}

	return batch, nil
}

// PublishBatch handles the transformation and publishing of a single batch
func (bh *BatchHandler) PublishBatch(notification *models.ConnectNotification, batch *datasetpb.DataBatchLabeled) error {
	// Prepare batches
	unlabeledBatch, labeledBatch := bh.prepareBatches(batch)

	// Marshal batches
	unlabeledBody, labeledBody, err := bh.marshalBatches(unlabeledBatch, labeledBatch)
	if err != nil {
		return err
	}

	// Publish batches
	return bh.publishBatches(notification, unlabeledBody, labeledBody, batch.GetBatchIndex())
}

// prepareBatches creates the unlabeled and labeled protobuf batches
func (bh *BatchHandler) prepareBatches(batch *datasetpb.DataBatchLabeled) (*datasetpb.DataBatchUnlabeled, *datasetpb.DataBatchLabeled) {
	unlabeledBatch := &datasetpb.DataBatchUnlabeled{
		Data:        batch.GetData(),
		BatchIndex:  batch.GetBatchIndex(),
		IsLastBatch: batch.GetIsLastBatch(),
	}

	labeledBatch := &datasetpb.DataBatchLabeled{
		Data:        batch.GetData(),
		BatchIndex:  batch.GetBatchIndex(),
		IsLastBatch: batch.GetIsLastBatch(),
		Labels:      batch.GetLabels(),
	}

	return unlabeledBatch, labeledBatch
}

// marshalBatches serializes the batches to protobuf
func (bh *BatchHandler) marshalBatches(unlabeledBatch *datasetpb.DataBatchUnlabeled, labeledBatch *datasetpb.DataBatchLabeled) ([]byte, []byte, error) {
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

// publishBatches publishes both unlabeled and labeled batches to RabbitMQ
func (bh *BatchHandler) publishBatches(notification *models.ConnectNotification, unlabeledBody, labeledBody []byte, batchIndex int32) error {
	routingKeys := []struct {
		key  string
		body []byte
		typ  string
	}{
		{fmt.Sprintf("%s.unlabeled", notification.ClientId), unlabeledBody, "unlabeled"},
		{fmt.Sprintf("%s.labeled", notification.ClientId), labeledBody, "labeled"},
	}

	for _, rk := range routingKeys {
		if err := bh.middleware.Publish(rk.key, rk.body, config.DATASET_EXCHANGE); err != nil {
			bh.logger.WithFields(logrus.Fields{
				"client_id":   notification.ClientId,
				"model_type":  notification.ModelType,
				"batch_index": batchIndex,
				"routing_key": rk.key,
				"batch_type":  rk.typ,
				"error":       err.Error(),
			}).Error("Failed to publish batch to exchange")
			return fmt.Errorf("failed to publish %s batch with routing key %s: %w", rk.typ, rk.key, err)
		}
	}

	return nil
}
