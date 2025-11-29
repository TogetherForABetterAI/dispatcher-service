package server

import (
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	"github.com/sirupsen/logrus"
)

// ClientManager handles processing client data requests
type ClientManager struct {
	userID       string // Will be set when HandleClient is called
	logger       *logrus.Logger
	middleware   middleware.MiddlewareInterface
	dbClient     DBClient
	publisher    *middleware.Publisher // Reused publisher from worker
	batchHandler *BatchHandler
}

type ClientManagerInterface interface {
	HandleClient(notification *models.ConnectNotification) error
	Stop()
}

// NewClientManager creates a new client manager
func NewClientManager(cfg config.Interface, mw middleware.MiddlewareInterface, dbClient DBClient, publisher *middleware.Publisher) *ClientManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &ClientManager{
		logger:     logger,
		middleware: mw,
		dbClient:   dbClient,
		publisher:  publisher,
		userID:     "", // Will be set in HandleClient
	}
}

// HandleClient processes a client notification by fetching batches from DB and publishing to client queue
func (c *ClientManager) HandleClient(notification *models.ConnectNotification) error {
	// Set UserID from notification
	c.userID = notification.UserID

	c.logger.WithFields(logrus.Fields{
		"user_id":    notification.UserID,
		"session_id": notification.SessionId,
	}).Info("Starting to handle client notification")

	dispatcherToCalibrationQueue := fmt.Sprintf(config.DISPATCHER_TO_CALIBRATION_QUEUE, notification.UserID)
	dispatcherToClientQueue := fmt.Sprintf(config.DISPATCHER_TO_CLIENT_QUEUE, notification.UserID)

	if err := c.middleware.DeclareQueue(dispatcherToCalibrationQueue); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", dispatcherToClientQueue, err)
	}

	// Create batch handler with the reused publisher from worker
	c.batchHandler = NewBatchHandler(c.publisher, c.dbClient, c.logger, dispatcherToClientQueue, dispatcherToCalibrationQueue)

	// Start processing batches
	return c.batchHandler.Start(notification)
}

func (c *ClientManager) Stop() {
	c.logger.Info("Stopping ClientManager for client ", c.userID)
	if c.batchHandler != nil {
		c.batchHandler.Stop()
	}
}
