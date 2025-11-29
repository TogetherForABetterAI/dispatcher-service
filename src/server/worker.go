package server

import (
	"context"
	"encoding/json"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Worker handles processing individual client notification messages
type Worker struct {
	id                   int
	config               config.Interface
	middleware           middleware.MiddlewareInterface
	dbClient             DBClient
	clientManagerFactory ClientManagerFactory
	logger               *logrus.Logger
	publisher            *middleware.Publisher // Reused publisher for this worker

	// Current active client manager (only one at a time per worker)
	clientManager ClientManagerInterface
}

// NewWorker creates a new worker instance
func NewWorker(
	id int,
	cfg config.Interface,
	middleware middleware.MiddlewareInterface,
	dbClient DBClient,
	factory ClientManagerFactory,
	logger *logrus.Logger,
) *Worker {
	return &Worker{
		id:                   id,
		config:               cfg,
		middleware:           middleware,
		dbClient:             dbClient,
		clientManagerFactory: factory,
		logger:               logger,
		clientManager:        nil,
	}
}

// Start begins processing jobs from the jobs channel
func (w *Worker) Start(jobs <-chan amqp.Delivery) {
	w.logger.WithField("worker_id", w.id).Info("Worker started")

	// Create publisher once for this worker
	publisher, err := middleware.NewPublisher(w.middleware.Conn())
	if err != nil {
		w.logger.WithFields(logrus.Fields{
			"worker_id": w.id,
			"error":     err.Error(),
		}).Fatal("Failed to create publisher for worker")
		return
	}
	w.publisher = publisher

	// Ensure publisher is closed when worker stops
	defer func() {
		w.publisher.Close()
		w.logger.WithField("worker_id", w.id).Info("Worker publisher closed")
	}()

	for msg := range jobs {
		w.logger.WithField("worker_id", w.id).Debug("Worker picked up a job")
		w.safeProcessMessage(msg)
		w.logger.WithField("worker_id", w.id).Debug("Worker finished a job")
	}

	w.logger.WithField("worker_id", w.id).Info("Worker shutting down")
}

// safeProcessMessage wraps processMessage with panic recovery
func (w *Worker) safeProcessMessage(msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.WithFields(logrus.Fields{
				"worker_id":   w.id,
				"panic_error": r,
				"body":        string(msg.Body),
			}).Error("Recovered from panic while processing message")

			// Nack the message without requeuing to avoid infinite loops
			msg.Nack(false, false)
		}
	}()

	// If a panic occurs, it will be caught by the defer above
	w.processMessage(msg)
}

// processMessage processes a single client notification message
func (w *Worker) processMessage(msg amqp.Delivery) {
	// Parse the notification
	var notification models.ConnectNotification
	if err := json.Unmarshal(msg.Body, &notification); err != nil {
		w.logger.WithFields(logrus.Fields{
			"worker_id": w.id,
			"error":     err.Error(),
			"body":      string(msg.Body),
		}).Error("Failed to unmarshal client notification")
		msg.Nack(false, false) // Don't requeue invalid messages
		return
	}

	// Validate notification
	if notification.UserID == "" {
		w.logger.WithFields(logrus.Fields{
			"worker_id":    w.id,
			"notification": notification,
		}).Error("Client notification missing user_id")
		msg.Nack(false, false) // Don't requeue invalid messages
		return
	}

	w.logger.WithFields(logrus.Fields{
		"worker_id":               w.id,
		"user_id":                 notification.UserID,
		"session_id":              notification.SessionId,
		"total_batches_generated": notification.TotalBatchesGenerated,
	}).Info("Processing new client notification")

	// Create client manager for this notification
	clientManager := w.clientManagerFactory(w.config, w.middleware, w.dbClient, w.publisher)

	// Set as current active client manager
	w.clientManager = clientManager

	// Process the client
	if err := clientManager.HandleClient(&notification); err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			w.logger.WithFields(logrus.Fields{
				"worker_id": w.id,
				"user_id":   notification.UserID,
				"error":     err.Error(),
			}).Warn("Client processing was cancelled")
			return
		}
		w.logger.WithFields(logrus.Fields{
			"worker_id": w.id,
			"user_id":   notification.UserID,
			"error":     err.Error(),
		}).Error("Failed to process client")
		msg.Nack(false, true) // Requeue on processing error
	} else {
		w.logger.WithFields(logrus.Fields{
			"worker_id": w.id,
			"user_id":   notification.UserID,
		}).Info("Successfully completed client processing")
		msg.Ack(false) // Acknowledge successful processing
	}
}

// Stop signals the worker to stop its current client processing
func (w *Worker) Stop() {
	if w.clientManager != nil {
		w.logger.WithField("worker_id", w.id).Info("Stopping current client manager")
		w.clientManager.Stop()
	} else {
		w.logger.WithField("worker_id", w.id).Debug("No active client manager to stop")
	}
}
