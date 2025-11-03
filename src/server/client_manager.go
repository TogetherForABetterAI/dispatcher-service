package server

import (
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/grpc"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// ClientManager handles processing client data requests
type ClientManager struct {
	clientID           string
	datasetServiceAddr string
	logger             *logrus.Logger
	maxRetries         int
	batchSize          int32
	conn               *amqp.Connection
	middleware         middleware.MiddlewareInterface
	batchHandler       *BatchHandler
}

type ClientManagerInterface interface {
	HandleClient(notification *models.ConnectNotification) error
	Stop()
}

// NewClientManager creates a new client manager
func NewClientManager(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) *ClientManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &ClientManager{
		datasetServiceAddr: cfg.GetGrpcConfig().GetDatasetServiceAddr(),
		logger:             logger,
		maxRetries:         cfg.GetMiddlewareConfig().GetMaxRetries(),
		batchSize:          cfg.GetGrpcConfig().GetBatchSize(),
		conn:               mw.Conn(),
		middleware:         mw,
		batchHandler:       nil,
		clientID:           clientID,
	}
}

// HandleClient processes a client notification by fetching and publishing dataset batches
func (c *ClientManager) HandleClient(notification *models.ConnectNotification) error {

	// Create RabbitMQ publisher using shared connection
	publisher, err := middleware.NewPublisher(c.conn)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
	}
	defer publisher.Close()

	// Create and bind queues for this client
	if err := c.createAndBindClientQueues(notification.ClientId); err != nil {
		return err
	}

	// Create gRPC client for dataset service
	grpcClient, err := grpc.NewClient(c.datasetServiceAddr)
	if err != nil {
		return fmt.Errorf("failed to create dataset service client: %w", err)
	}

	// Create and start batch handler
	c.batchHandler = NewBatchHandler(publisher, grpcClient, notification.ModelType, c.batchSize, c.logger)
	return c.batchHandler.Start(notification)
}

// createAndBindClientQueues creates and binds labeled and unlabeled queues for a client
func (c *ClientManager) createAndBindClientQueues(clientID string) error {
	labeledQueueName := fmt.Sprintf("%s_labeled_queue", clientID)
	unlabeledQueueName := fmt.Sprintf("%s_unlabeled_queue", clientID)
	routingKeyLabeled := fmt.Sprintf("%s.labeled", clientID)
	routingKeyUnlabeled := fmt.Sprintf("%s.unlabeled", clientID)

	// Declare and bind labeled queue
	if err := c.middleware.DeclareQueue(labeledQueueName); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", labeledQueueName, err)
	}
	if err := c.middleware.BindQueue(labeledQueueName, config.DATASET_EXCHANGE, routingKeyLabeled); err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s with routing key %s: %w", labeledQueueName, config.DATASET_EXCHANGE, routingKeyLabeled, err)
	}

	// Declare and bind unlabeled queue
	if err := c.middleware.DeclareQueue(unlabeledQueueName); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", unlabeledQueueName, err)
	}
	if err := c.middleware.BindQueue(unlabeledQueueName, config.DATASET_EXCHANGE, routingKeyUnlabeled); err != nil {
		return fmt.Errorf("failed to bind queue %s to exchange %s with routing key %s: %w", unlabeledQueueName, config.DATASET_EXCHANGE, routingKeyUnlabeled, err)
	}

	return nil
}

func (c *ClientManager) Stop() {
	c.logger.Info("Stopping ClientManager for client ", c.clientID)
	if c.batchHandler != nil {
		c.batchHandler.Stop()
	}
}
