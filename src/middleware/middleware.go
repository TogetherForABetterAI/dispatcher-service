package middleware

import (
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

type Middleware struct {
	conn             *amqp.Connection
	channel          *amqp.Channel
	confirms_chan    chan amqp.Confirmation
	logger           *logrus.Logger
	MiddlewareConfig *config.MiddlewareConfig
}

const MAX_RETRIES = 5

func NewMiddleware(cfg *config.MiddlewareConfig) (*Middleware, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		cfg.GetUsername(), cfg.GetPassword(), cfg.GetHost(), cfg.GetPort())

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	confirms_chan := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.Qos(1, 0, false); err != nil {
		return nil, err
	}

	// Declare required exchanges here
	mw := &Middleware{channel: ch}
	if err := mw.DeclareExchange(config.DATASET_EXCHANGE, "direct"); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange %s: %w", config.DATASET_EXCHANGE, err)
	}

	logger.WithFields(logrus.Fields{
		"host": cfg.GetHost(),
		"port": cfg.GetPort(),
		"user": cfg.GetUsername(),
	}).Info("Connected to RabbitMQ")

	return &Middleware{
		conn:             conn,
		channel:          ch,
		confirms_chan:    confirms_chan,
		logger:           logger,
		MiddlewareConfig: cfg,
	}, nil
}

func (m *Middleware) DeclareQueue(queueName string) error {
	_, err := m.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	return err
}

func (m *Middleware) DeclareExchange(exchangeName string, exchangeType string) error {
	return m.channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		false, // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
}

func (m *Middleware) BindQueue(queueName, exchangeName, routingKey string) error {
	return m.channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
}

// StopConsuming cancels all consumers to stop receiving new messages
func (m *Middleware) StopConsuming(consumerTag string) error {
	if m.channel != nil {
		if err := m.channel.Cancel(consumerTag, false); err != nil {
			m.logger.WithError(err).Warnf("Failed to cancel consumer '%s'", consumerTag)
			return err
		}
	}
	return nil
}

// SetQoS configures the prefetch count for the channel
func (mw *Middleware) SetQoS(prefetchCount int) error {
	return mw.channel.Qos(
		prefetchCount, // prefetch count - number of messages without ack
		0,             // prefetch size - 0 means no limit on message size
		false,
	)
}

func (m *Middleware) BasicConsume(queueName string, consumerTag string) (<-chan amqp.Delivery, error) {
	msgs, err := m.channel.Consume(
		queueName,
		"",    // consumer
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func (m *Middleware) Close() {
	if m.channel != nil {
		if err := m.channel.Close(); err != nil && !m.conn.IsClosed() {
			m.logger.WithError(err).Warn("Failed to close RabbitMQ channel")
		}
	}
	if m.conn != nil && !m.conn.IsClosed() {
		if err := m.conn.Close(); err != nil {
			m.logger.WithError(err).Warn("Failed to close RabbitMQ connection")
		}
	}
	m.logger.Info("RabbitMQ connection closed")
}

// Conn returns the underlying connection for reuse
func (m *Middleware) Conn() *amqp.Connection {
	return m.conn
}
