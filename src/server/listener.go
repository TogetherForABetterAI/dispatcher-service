package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/middleware"
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
type ClientManagerFactory func(config.Interface, middleware.MiddlewareInterface, DBClient, *middleware.Publisher) ClientManagerInterface

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

	// Track active workers
	workers []*Worker

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
	// Its size == pool size, matching the prefetch count.
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
		workers:              make([]*Worker, 0, cfg.GetWorkerPoolSize()),
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

	// Start worker pool
	for i := 0; i < poolSize; i++ {
		worker := NewWorker(i, l.config, l.middleware, l.dbClient, l.clientManagerFactory, l.logger)
		l.workers = append(l.workers, worker)

		l.wg.Add(1)
		go func(w *Worker) {
			defer l.wg.Done()
			w.Start(l.jobs)
		}(worker)
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

func (l *Listener) InterruptClients() {
	l.cancel() // stop processing new messages

	l.logger.Info("Listener stopping - signaling workers to stop.")
	for _, worker := range l.workers {
		worker.Stop() // Signal each worker to stop its current client
	}

	l.wg.Wait() // wait for workers to finish processing
	l.logger.Info("All workers have finished processing.")
}

func (l *Listener) GetConsumerTag() string {
	return l.consumerTag
}

