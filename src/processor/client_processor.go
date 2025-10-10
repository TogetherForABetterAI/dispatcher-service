package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ClientDataProcessor handles processing client data requests
type ClientProcessor interface {
	ProcessClient(ctx context.Context, req *clientpb.NewClientRequest) error
}

type ClientDataProcessor struct {
	logger            *logrus.Logger
	datasetGrpcClient DatasetServiceClient
	middleware        IMiddleware
	grpcConfig        config.GrpcConfig
}

// NewClientDataProcessor creates a new client data processor
func NewClientDataProcessor(
	datasetGrpcClient DatasetServiceClient,
	middleware IMiddleware,
	logger *logrus.Logger,
	config *config.GrpcConfig,
) *ClientDataProcessor {
	if logger == nil {
		logger = logrus.New()
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	return &ClientDataProcessor{
		datasetGrpcClient: datasetGrpcClient,
		middleware:        middleware,
		logger:            logger,
		grpcConfig:        *config,
	}
}

func NewClientDataProcessorFromConfig(globalConfig *config.GlobalConfig, logger *logrus.Logger) (*ClientDataProcessor, func(), error) {
	middleware, err := middleware.NewMiddleware(globalConfig.MiddlewareConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create middleware publisher: %w", err)
	}

	// Declare the dataset exchange on startup
	if err := middleware.DeclareExchange(config.DATASET_EXCHANGE, "topic"); err != nil {
		middleware.Close()
		return nil, nil, fmt.Errorf("failed to declare exchange '%s': %w", config.DATASET_EXCHANGE, err)
	}
	logger.WithField("exchange", config.DATASET_EXCHANGE).Info("Successfully declared dataset exchange")

	datasetGrpcClient, err := grpc.NewClient(globalConfig.GrpcConfig.DatasetAddr)
	if err != nil {
		middleware.Close()
		return nil, nil, fmt.Errorf("failed to create dataset service client: %w", err)
	}

	processor := NewClientDataProcessor(datasetGrpcClient, middleware, logger, globalConfig.GrpcConfig)

	cleanup := func() {
		middleware.Close()
		datasetGrpcClient.Close()
	}

	return processor, cleanup, nil
}

// ProcessClient implements the ClientProcessor interface
func (p *ClientDataProcessor) ProcessClient(ctx context.Context, req *clientpb.NewClientRequest) error {
	// Use client_id as routing key
	routingKey := req.ClientId

	p.logger.WithFields(logrus.Fields{
		"client_id":   req.ClientId,
		"routing_key": routingKey,
		"model_type":  req.ModelType,
	}).Info("Starting client data processing")

	datasetName := req.ModelType

	p.logger.WithFields(logrus.Fields{
		"client_id":    req.ClientId,
		"dataset_name": datasetName,
		"batch_size":   p.grpcConfig.BatchSize,
	}).Info("Starting batch fetching loop")

	// Start processing batches
	batchIndex := int32(0)
	for {
		// Fetch batch from dataset service with timeout
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		batchReq := &datasetpb.GetBatchRequest{
			DatasetName: datasetName, // Use per-client dataset
			BatchSize:   p.grpcConfig.BatchSize,
			BatchIndex:  batchIndex,
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":    req.ClientId,
			"batch_index":  batchIndex,
			"dataset_name": datasetName,
			"batch_size":   p.grpcConfig.BatchSize,
		}).Debug("Requesting batch from dataset service")

		batch, err := p.datasetGrpcClient.GetBatch(batchCtx, batchReq)
		cancel()

		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"client_id":   req.ClientId,
				"batch_index": batchIndex,
				"error":       err.Error(),
			}).Error("Failed to fetch batch from dataset service")
			return fmt.Errorf("failed to fetch batch %d: %w", batchIndex, err)
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":     req.ClientId,
			"batch_index":   batchIndex,
			"data_size":     len(batch.GetData()),
			"labels_count":  len(batch.GetLabels()),
			"is_last_batch": batch.GetIsLastBatch(),
		}).Debug("Received batch from dataset service")

		if err := p.publishBatch(routingKey, batch); err != nil {
			p.logger.WithFields(logrus.Fields{
				"client_id":   req.ClientId,
				"batch_index": batchIndex,
				"error":       err.Error(),
			}).Error("Failed to publish batch")
			return fmt.Errorf("failed to publish batch %d: %w", batchIndex, err)
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":     req.ClientId,
			"batch_index":   batchIndex,
			"routing_key":   routingKey,
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

// handles the marshaling and publishing of both labeled and unlabeled batches
func (p *ClientDataProcessor) publishBatch(routingKey string, batch *datasetpb.DataBatchLabeled) error {
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

	unlabeledBody, err := proto.Marshal(unlabeledProtoBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal unlabeled batch: %w", err)
	}
	labeledBody, err := proto.Marshal(labeledProtoBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal labeled batch: %w", err)
	}

	routingKeys := []struct {
		key  string
		body []byte
	}{
		{fmt.Sprintf("%s.unlabeled", routingKey), unlabeledBody},
		{fmt.Sprintf("%s.labeled", routingKey), labeledBody},
	}

	for _, rk := range routingKeys {
		if err := p.middleware.Publish(rk.key, rk.body, config.DATASET_EXCHANGE); err != nil {
			return fmt.Errorf("failed to publish batch with routing key %s: %w", rk.key, err)
		}
	}

	return nil
}
