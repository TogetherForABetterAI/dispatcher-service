package server

import (
	"context"
	"fmt"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/mlops-eval/data-dispatcher-service/src/models"
	"github.com/sirupsen/logrus"
)

// ClientManager handles processing client data requests
type ClientManager struct {
	datasetServiceAddr string
	logger             *logrus.Logger
	maxRetries         int
	datasetName        string
	batchSize          int32
}

// NewClientManager creates a new client manager
func NewClientManager(cfg config.GlobalConfig) *ClientManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &ClientManager{
		datasetServiceAddr: cfg.GetGrpcConfig().GetDatasetServiceAddr(),
		logger:             logger,
		maxRetries:         cfg.GetMiddlewareConfig().GetMaxRetries(),
		datasetName:        cfg.GetGrpcConfig().GetDatasetName(),
		batchSize:          cfg.GetGrpcConfig().GetBatchSize(),
	}
}

// HandleClient processes a client notification by fetching and publishing dataset batches
func (c *ClientManager) HandleClient(ctx context.Context, notification *models.ConnectNotification) error {
	c.logger.WithFields(logrus.Fields{
		"client_id": notification.ClientId,
	}).Info("Starting client data processing")

	// Create RabbitMQ middleware using global config
	middlewareInstance, err := middleware.NewMiddleware(config.Config.GetMiddlewareConfig())
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ middleware: %w", err)
	}
	defer middlewareInstance.Close()

	// Declare exchange
	if err := middlewareInstance.DeclareExchange(config.DATASET_EXCHANGE, "direct"); err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Create gRPC client for dataset service
	grpcClient, err := grpc.NewClient(c.datasetServiceAddr)
	if err != nil {
		return fmt.Errorf("failed to create dataset service client: %w", err)
	}

	// Create and start batch handler
	batchHandler := NewBatchHandler(middlewareInstance, grpcClient, c.datasetName, c.batchSize, c.logger)
	return batchHandler.Start(ctx, notification)
}
