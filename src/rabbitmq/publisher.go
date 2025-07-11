package rabbitmq

import (
	"context"
	"fmt"
	"time"

	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// Publisher handles RabbitMQ publishing operations
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *logrus.Logger
}

// Config holds RabbitMQ connection configuration
type Config struct {
	Host     string
	Port     int32
	Username string
	Password string
}

// BatchData represents the data structure to be published to RabbitMQ
type BatchData struct {
	ClientID    string    `json:"client_id"`
	BatchIndex  int32     `json:"batch_index"`
	Data        []byte    `json:"data"`
	IsLastBatch bool      `json:"is_last_batch"`
	Timestamp   time.Time `json:"timestamp"`
}

// NewPublisher creates a new RabbitMQ publisher with the given configuration
func NewPublisher(config Config) (*Publisher, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		config.Username, config.Password, config.Host, config.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	publisher := &Publisher{
		conn:    conn,
		channel: channel,
		logger:  logger,
	}

	logger.WithFields(logrus.Fields{
		"host": config.Host,
		"port": config.Port,
		"user": config.Username,
	}).Info("Connected to RabbitMQ")

	return publisher, nil
}

// PublishBatch publishes a data batch to the dataset-exchange using the provided routing key
func (p *Publisher) PublishBatch(ctx context.Context, routingKey string, batch *BatchData) error {
	// Convert BatchData to protobuf DataBatch
	protoBatch := &datasetpb.DataBatch{
		Data:        batch.Data,
		BatchIndex:  batch.BatchIndex,
		IsLastBatch: batch.IsLastBatch,
	}

	// Marshal the batch data to protobuf
	body, err := proto.Marshal(protoBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch data to protobuf: %w", err)
	}

	// Exchange name
	datasetExchange := "dataset-exchange"

	// Publish to dataset-exchange
	err = p.channel.PublishWithContext(
		ctx,
		datasetExchange, // exchange
		routingKey,      // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "application/x-protobuf",
			Body:         body,
			Timestamp:    batch.Timestamp,
			DeliveryMode: amqp.Persistent, // Make message persistent
			Headers: amqp.Table{
				"client_id":     batch.ClientID,
				"batch_index":   batch.BatchIndex,
				"is_last_batch": batch.IsLastBatch,
			},
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish message to %s exchange: %w", datasetExchange, err)
	}

	p.logger.WithFields(logrus.Fields{
		"routing_key":   routingKey,
		"client_id":     batch.ClientID,
		"batch_index":   batch.BatchIndex,
		"is_last_batch": batch.IsLastBatch,
		"data_size":     len(batch.Data),
		"exchange":      datasetExchange,
	}).Debug("Published batch to dataset-exchange")

	return nil
}

// PublishBatchWithRetry publishes a batch with retry logic
func (p *Publisher) PublishBatchWithRetry(ctx context.Context, routingKey string, batch *BatchData, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			p.logger.WithFields(logrus.Fields{
				"attempt":     attempt + 1,
				"max_retries": maxRetries + 1,
				"routing_key": routingKey,
				"client_id":   batch.ClientID,
			}).Warn("Retrying to publish batch")
		}

		lastErr = p.PublishBatch(ctx, routingKey, batch)
		if lastErr == nil {
			if attempt > 0 {
				p.logger.WithFields(logrus.Fields{
					"attempt":     attempt + 1,
					"routing_key": routingKey,
					"client_id":   batch.ClientID,
				}).Info("Successfully published batch after retry")
			}
			return nil
		}

		p.logger.WithFields(logrus.Fields{
			"attempt":     attempt + 1,
			"error":       lastErr.Error(),
			"routing_key": routingKey,
			"client_id":   batch.ClientID,
		}).Error("Failed to publish batch")
	}

	return fmt.Errorf("failed to publish after %d attempts: %w", maxRetries+1, lastErr)
}

// Close closes the RabbitMQ connection and channel
func (p *Publisher) Close() error {
	var channelErr, connErr error

	if p.channel != nil {
		channelErr = p.channel.Close()
	}

	if p.conn != nil {
		connErr = p.conn.Close()
	}

	if channelErr != nil {
		return fmt.Errorf("failed to close channel: %w", channelErr)
	}

	if connErr != nil {
		return fmt.Errorf("failed to close connection: %w", connErr)
	}

	p.logger.Info("Closed RabbitMQ connection")
	return nil
}

// IsConnected checks if the RabbitMQ connection is still alive
func (p *Publisher) IsConnected() bool {
	return p.conn != nil && !p.conn.IsClosed()
}
