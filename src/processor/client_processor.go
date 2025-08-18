package processor

import (
	"context"
	"fmt"
	"time"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ClientDataProcessor handles processing client data requests
type ClientDataProcessor struct {
	datasetServiceAddr string
	logger             *logrus.Logger
	maxRetries         int
	rabbitConfig       *config.MiddlewareConfig
	grpcConfig         *config.GrpcConfig
}

// NewClientDataProcessor creates a new client data processor
func NewClientDataProcessor() *ClientDataProcessor {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})


	config := config.InitializeConfig()

	return &ClientDataProcessor{
		datasetServiceAddr: config.GrpcConfig.DatasetAddr,
		logger:             logger,
		maxRetries:         config.MiddlewareConfig.MaxRetries,
		rabbitConfig:       config.MiddlewareConfig,
		grpcConfig:         config.GrpcConfig,
	}
}

// ProcessClient implements the ClientProcessor interface
func (p *ClientDataProcessor) ProcessClient(ctx context.Context, req *clientpb.NewClientRequest) error {
	p.logger.WithFields(logrus.Fields{
		"client_id":   req.ClientId,
		"routing_key": req.RoutingKey,
	}).Info("Starting client data processing")

	// Create RabbitMQ middleware
	middleware, err := middleware.NewMiddleware(p.rabbitConfig)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ middleware: %w", err)
	}
	defer middleware.Close()

	// Create gRPC client for dataset service
	grpcClient, err := grpc.NewClient(p.datasetServiceAddr)
	if err != nil {
		return fmt.Errorf("failed to create dataset service client: %w", err)
	}


	// Start processing batches
	batchIndex := int32(0)
	for {
		// Fetch batch from dataset service with timeout
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		batchReq := &datasetpb.GetBatchRequest{
			DatasetName: p.grpcConfig.DatasetName,
			BatchSize:   p.grpcConfig.BatchSize,
			BatchIndex:  batchIndex,
		}

		batch, err := grpcClient.GetBatch(batchCtx, batchReq)
		cancel()

		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"client_id":   req.ClientId,
				"batch_index": batchIndex,
				"error":       err.Error(),
			}).Error("Failed to fetch batch from dataset service")
			return fmt.Errorf("failed to fetch batch %d: %w", batchIndex, err)
		}
		// Prepare unlabeled and labeled batches
		unlabeledProtoBatch := &datasetpb.DataBatchUnlabeled{
			Data:        batch.GetData(),
			BatchIndex:  batch.GetBatchIndex(),
			IsLastBatch: batch.GetIsLastBatch(),
		}
		labeledProtoBatch := &datasetpb.DataBatchLabeled{
			Data:        batch.GetData(),
			BatchIndex:  batch.GetBatchIndex(),
			IsLastBatch: batch.GetIsLastBatch(),
			Labels:      batch.GetLabels(),
		}

		// Marshal batches
		unlabeledBody, err := proto.Marshal(unlabeledProtoBatch)
		if err != nil {
			return fmt.Errorf("failed to marshal unlabeledProtoBatch: %w", err)
		}
		labeledBody, err := proto.Marshal(labeledProtoBatch)
		if err != nil {
			return fmt.Errorf("failed to marshal labeledProtoBatch: %w", err)
		}

		// Publish batches to exchanges
		routingKeys := []struct {
			key   string
			body  []byte
		}{
			{fmt.Sprintf("%s.unlabeled", req.RoutingKey), unlabeledBody},
			{fmt.Sprintf("%s.labeled", req.RoutingKey), labeledBody},
		}

		for _, rk := range routingKeys {
			if err := middleware.Publish(rk.key, rk.body, config.DATASET_EXCHANGE); err != nil {
				p.logger.WithFields(logrus.Fields{
					"client_id":   req.ClientId,
					"batch_index": batchIndex,
					"routing_key": rk.key,
					"error":       err.Error(),
				}).Error("Failed to publish batch to exchanges")
				return fmt.Errorf("failed to publish batch %d with routing key %s: %w", batchIndex, rk.key, err)
			}
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":     req.ClientId,
			"batch_index":   batchIndex,
			"routing_key":   req.RoutingKey,
			"is_last_batch": batch.GetIsLastBatch(),
			"data_size":     len(batch.GetData()),
		}).Info("Successfully published batch to both exchanges")

		// Check if this was the last batch
		if batch.GetIsLastBatch() {
			p.logger.WithFields(logrus.Fields{
				"client_id":     req.ClientId,
				"total_batches": batchIndex + 1,
			}).Info("Completed data processing for client")
			break
		}

		batchIndex++

		// Add small delay between batches to avoid overwhelming the services
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return nil
}
