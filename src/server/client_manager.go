package server

import (
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// ClientManager handles processing client data requests
type ClientManager struct {
	clientID     string
	logger       *logrus.Logger
	conn         *amqp.Connection
	middleware   middleware.MiddlewareInterface
	dbClient     DBClient
	batchHandler *BatchHandler
}

type ClientManagerInterface interface {
	HandleClient(notification *models.ConnectNotification) error
	Stop()
}

// NewClientManager creates a new client manager
func NewClientManager(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, clientID string) *ClientManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &ClientManager{
		logger:     logger,
		conn:       mw.Conn(),
		middleware: mw,
		clientID:   clientID,
		dbClient:   dbClient,
	}
}

// HandleClient processes a client notification by fetching batches from DB and publishing to client queue
func (c *ClientManager) HandleClient(notification *models.ConnectNotification) error {
	c.logger.WithFields(logrus.Fields{
		"client_id":  notification.ClientId,
		"session_id": notification.SessionId,
	}).Info("Starting to handle client notification")

	// Create RabbitMQ publisher using shared connection
	publisher, err := middleware.NewPublisher(c.conn)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
	}
	defer publisher.Close()

	dispatcherToCalibrationQueue := fmt.Sprintf("%s_labeled_queue", notification.ClientId)
	dispatcherToClientQueue := fmt.Sprintf("%s_dispatcher_queue", notification.ClientId)

	if err := c.middleware.DeclareQueue(dispatcherToCalibrationQueue); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", dispatcherToClientQueue, err)
	}

	// Create batch handler
	c.batchHandler = NewBatchHandler(publisher, c.dbClient, c.logger, dispatcherToClientQueue, dispatcherToCalibrationQueue)

	// Start processing batches
	return c.batchHandler.Start(notification)
}

func (c *ClientManager) Stop() {
	c.logger.Info("Stopping ClientManager for client ", c.clientID)
	if c.batchHandler != nil {
		c.batchHandler.Stop()
	}
}