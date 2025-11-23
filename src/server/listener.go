package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	// "time" // Ya no es necesario

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// DBClient defines the interface for database operations
type DBClient interface {
	GetPendingBatches(ctx context.Context, sessionID string) ([]db.Batch, error)
	GetPendingBatchesLimit(ctx context.Context, sessionID string, limit int) ([]db.Batch, error)
	MarkBatchAsEnqueued(ctx context.Context, batchID string) error
	MarkBatchesAsEnqueued(ctx context.Context, sessionIDs []string, batchIndices []int) error
	Close()
}

// this type definition can be used if you want to inject different client manager implementations
type ClientManagerFactory func(config.Interface, middleware.MiddlewareInterface, DBClient, string) ClientManagerInterface

// Listener handles listening for new client notifications and processing them
type Listener struct {
	middleware  middleware.MiddlewareInterface
	logger      *logrus.Logger
	queueName   string
	jobs        chan amqp.Delivery // Channel for worker pool
	wg          sync.WaitGroup
	consumerTag string
	config      config.Interface
	dbClient    DBClient // Shared database client

	clientsMutex  sync.RWMutex
	activeClients map[string]ClientManagerInterface

	// Context and cancellation for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	clientManagerFactory ClientManagerFactory
}

// NewListener creates a new listener with the provided client manager and logger
func NewListener(
	middleware middleware.MiddlewareInterface,
	cfg config.Interface,
	dbClient DBClient,
	factory ClientManagerFactory,
) *Listener {

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// This channel will buffer jobs for the workers
	// Its size = pool size, matching the prefetch count.
	jobs := make(chan amqp.Delivery, cfg.GetWorkerPoolSize())

	// Create an internal context for managing the listener's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	return &Listener{
		middleware:           middleware,
		logger:               logger,
		queueName:            config.CONNECTION_QUEUE_NAME,
		jobs:                 jobs,
		config:               cfg,
		dbClient:             dbClient,
		ctx:                  ctx,
		cancel:               cancel,
		activeClients:        make(map[string]ClientManagerInterface),
		clientManagerFactory: factory,
		consumerTag:          cfg.GetPodName(), // Use POD_NAME as consumer tag
	}
}

// Start starts the listener and the worker pool
func (l *Listener) Start() error {
	l.logger.WithField("queue", l.queueName).Info("Starting listener...")

	// Setup RabbitMQ topology (exchange, queue, and binding)
	if err := l.middleware.SetupTopology(); err != nil {
		return fmt.Errorf("failed to setup RabbitMQ topology: %w", err)
	}

	// Set Prefetch Count (QoS)
	poolSize := l.config.GetWorkerPoolSize()
	if err := l.middleware.SetQoS(poolSize); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Get messages channel using POD_NAME as consumer tag
	msgs, err := l.middleware.BasicConsume(l.queueName, l.consumerTag)
	if err != nil {
		return fmt.Errorf("failed to start consuming messages: %w", err)
	}

	l.logger.WithFields(logrus.Fields{
		"consumer_tag": l.consumerTag,
		"queue":        l.queueName,
	}).Info("Consumer registered with unique tag")

	for i := 0; i < poolSize; i++ {
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
			close(l.jobs) // Close jobs channel to signal workers to stop once they finish current jobs
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
		l.safeProcessMessage(msg)

		l.logger.WithField("worker_id", id).Debug("Worker finished a job")
	}

	l.logger.WithField("worker_id", id).Info("Worker shutting down.")
}

// safeProcessMessage wraps processMessage with panic recovery
func (l *Listener) safeProcessMessage(msg amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			l.logger.WithFields(logrus.Fields{
				"panic_error": r,
				"body":        string(msg.Body),
			}).Error("Recovered from panic while processing message")

			// Nack the message without requeuing to avoid infinite loops
			msg.Nack(false, false)
		}
	}()

	// if a panic occurs, it will be caught by the defer above
	l.processMessage(msg)
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
		"client_id":               notification.ClientId,
		"session_id":              notification.SessionId,
		"total_batches_generated": notification.TotalBatchesGenerated,
	}).Info("Processing new client notification")

	clientID := notification.ClientId

	clientManager := l.clientManagerFactory(l.config, l.middleware, l.dbClient, clientID)
	l.clientsMutex.Lock()
	l.activeClients[clientID] = clientManager
	l.clientsMutex.Unlock()

	// Ensure client session is removed
	// regardless of processing outcome
	defer func() {
		l.clientsMutex.Lock()
		delete(l.activeClients, clientID)
		l.clientsMutex.Unlock()
		l.logger.WithField("client_id", clientID).Info("Client processing finished and session removed.")
	}()

	if err := clientManager.HandleClient(&notification); err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return
		}
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

func (l *Listener) InterruptClients() {
	l.cancel() // stop processing new messages

	l.clientsMutex.RLock()
	l.logger.Info("Listener stopping - signaling workers to stop.")
	for _, clientManager := range l.activeClients {
		clientManager.Stop() // interrupt ongoing processing
	}
	l.clientsMutex.RUnlock()

	l.wg.Wait() // wait for workers to finish processing
	l.logger.Info("All clients have finished processing.")
}

func (l *Listener) GetConsumerTag() string {
	return l.consumerTag
}
