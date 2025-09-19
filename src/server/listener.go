package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/mlops-eval/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Listener handles listening for new client notifications and processing them
type Listener struct {
	clientManager *ClientManager
	middleware    *middleware.Middleware
	logger        *logrus.Logger
}

// NewListener creates a new listener with the provided client manager and logger
func NewListener(clientManager *ClientManager, middleware *middleware.Middleware, logger *logrus.Logger) *Listener {
	return &Listener{
		clientManager: clientManager,
		middleware:    middleware,
		logger:        logger,
	}
}

// StartConsuming starts consuming messages from the specified queue
func (l *Listener) Start(queueName string, shutdown chan struct{}, clientWg *sync.WaitGroup) error {
	l.logger.WithField("queue", queueName).Info("Started consuming client notifications")

	// Start consuming messages using middleware's BasicConsume
	err := l.middleware.BasicConsume(queueName, func(msg amqp.Delivery) {
		// Spawn goroutine to handle each message
		go l.HandleMessage(msg, shutdown, clientWg)
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	return nil
}

// HandleMessage processes individual client notification messages
func (l *Listener) HandleMessage(msg amqp.Delivery, shutdown chan struct{}, clientWg *sync.WaitGroup) {
	// Add to wait group since this function runs in a goroutine
	clientWg.Add(1)
	defer clientWg.Done()

	l.logger.WithFields(logrus.Fields{
		"message_id": msg.MessageId,
		"timestamp":  msg.Timestamp,
	}).Debug("Received client notification message")

	// Parse the notification
	var notification models.ConnectNotification
	if err := json.Unmarshal(msg.Body, &notification); err != nil {
		l.logger.WithFields(logrus.Fields{
			"error": err.Error(),
			"body":  string(msg.Body),
		}).Error("Failed to unmarshal client notification")
		msg.Nack(false, false) // Don't requeue invalid messages
		return
	}

	// Validate notification
	if notification.ClientId == "" {
		l.logger.WithFields(logrus.Fields{
			"notification": notification,
		}).Error("Client notification missing client_id")
		msg.Nack(false, false) // Don't requeue invalid messages
		return
	}

	l.logger.WithFields(logrus.Fields{
		"client_id":      notification.ClientId,
		"inputs_format":  notification.InputsFormat,
		"outputs_format": notification.OutputsFormat,
	}).Info("Processing new client notification")

	// Create a context that can be cancelled on shutdown
	clientCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for shutdown signal
	go func() {
		select {
		case <-shutdown:
			cancel()
		case <-clientCtx.Done():
		}
	}()

	if err := l.clientManager.HandleClient(clientCtx, &notification); err != nil {
		l.logger.WithFields(logrus.Fields{
			"client_id": notification.ClientId,
			"error":     err.Error(),
		}).Error("Failed to process client")
		msg.Nack(false, true) // Requeue on processing error
	} else {
		l.logger.WithFields(logrus.Fields{
			"client_id": notification.ClientId,
		}).Info("Successfully completed client processing")
		msg.Ack(false) // Acknowledge successful processing
	}
}
