package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	// "time" // Ya no es necesario

	"github.com/data-dispatcher-service/src/config"

	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Listener handles listening for new client notifications and processing them
type Listener struct {
	clientManager *ClientManager
	middleware    *middleware.Middleware
	logger        *logrus.Logger
	queueName     string
	jobs          chan amqp.Delivery // Channel for worker pool
	wg            sync.WaitGroup
	consumerTag   string
	config        config.Interface
	// Context and cancellation for graceful shutdown
	ctx     context.Context
	cancel  context.CancelFunc
	monitor *ReplicaMonitor
}

// NewListener creates a new listener with the provided client manager and logger
func NewListener(
	clientManager *ClientManager,
	middleware *middleware.Middleware,
	cfg config.Interface,
	monitor *ReplicaMonitor,
) *Listener {

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// This channel will buffer jobs for the workers
	// Its size = pool size, matching the prefetch count.
	jobs := make(chan amqp.Delivery, cfg.GetWorkerPoolSize())

	// Create an internal context for managing the listener's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	return &Listener{
		clientManager: clientManager,
		middleware:    middleware,
		logger:        logger,
		queueName:     config.CONNECTION_QUEUE_NAME,
		jobs:          jobs,
		config:        cfg,
		ctx:           ctx,
		cancel:        cancel,
		monitor:       monitor,
	}
}

// Start starts the listener and the worker pool
func (l *Listener) Start() error {
	l.logger.WithField("queue", l.queueName).Info("Starting listener...")

	// Set Prefetch Count (QoS)
	poolSize := l.config.GetWorkerPoolSize()
	if err := l.middleware.SetQoS(poolSize); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Get messages channel
	l.consumerTag = l.config.GetConsumerTag()
	msgs, err := l.middleware.BasicConsume(l.queueName, l.consumerTag)
	if err != nil {
		return fmt.Errorf("failed to start consuming messages: %w", err)
	}

	for i := range poolSize {
		l.wg.Add(1)
		go l.worker(i)
	}
	l.logger.Info("Worker pool started.")

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				l.logger.Warn("Message channel closed (likely due to connection loss)")
				close(l.jobs)
				return nil
			}
			l.jobs <- msg

		case <-l.ctx.Done():
			l.logger.Info("Listener received shutdown signal. Stopping message consumption.")
			close(l.jobs)
			l.logger.Info("Jobs channel closed. Workers will finish processing remaining jobs.")
			return l.ctx.Err() // Return context error
		}
	}
}

// worker is a long-lived goroutine that processes jobs
func (l *Listener) worker(id int) {
	defer l.wg.Done()
	l.logger.WithField("worker_id", id).Info("Worker started")

	for msg := range l.jobs {
		l.logger.WithField("worker_id", id).Debug("Worker picked up a job")

		l.monitor.NotifyWorkerStart()
		l.processMessage(msg)
		l.monitor.NotifyWorkerFinish()

		l.logger.WithField("worker_id", id).Debug("Worker finished a job")
	}

	l.logger.WithField("worker_id", id).Info("Worker shutting down.")
}

// processMessage processes a single client notification message
func (l *Listener) processMessage(msg amqp.Delivery) {
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
		"model_type":     notification.ModelType,
	}).Info("Processing new client notification")

	// We pass the main listener context. If shutdown is triggered,
	// HandleClient should ideally respect this context and stop early.
	if err := l.clientManager.HandleClient(l.ctx, &notification); err != nil {
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

// Stop initiates a graceful shutdown
func (l *Listener) Stop() {
	l.logger.Info("Listener stopping - signaling workers to stop.")
	l.cancel()
	l.wg.Wait()
	l.logger.Info("Listener stopped successfully. All workers finished.")
}

func (l *Listener) GetConsumerTag() string {
	return l.consumerTag
}
