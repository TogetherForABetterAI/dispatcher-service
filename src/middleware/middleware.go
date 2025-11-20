package middleware

import (
	"fmt"
	"sync"

	"github.com/data-dispatcher-service/src/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// MiddlewareInterface defines the contract for middleware operations
type MiddlewareInterface interface {
	SetupTopology() error
	DeclareQueue(queueName string) error
	DeclareExchange(exchangeName string, exchangeType string) error
	BindQueue(queueName, exchangeName, routingKey string) error
	StopConsuming(consumerTag string) error
	SetQoS(prefetchCount int) error
	BasicConsume(queueName string, consumerTag string) (<-chan amqp.Delivery, error)
	Close()
	Conn() *amqp.Connection
}

type Middleware struct {
	conn             *amqp.Connection
	channel          *amqp.Channel
	confirms_chan    chan amqp.Confirmation
	logger           *logrus.Logger
	MiddlewareConfig *config.MiddlewareConfig
	mu               sync.Mutex
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

// SetupTopology configures the RabbitMQ topology (exchange, queue, and binding)
func (m *Middleware) SetupTopology() error {
	exchangeName := config.DISPATCHER_EXCHANGE
	queueName := config.CONNECTION_QUEUE_NAME
	routingKey := queueName

	// Declare the exchange
	if err := m.DeclareExchange(exchangeName, "direct"); err != nil {
		return fmt.Errorf("failed to declare exchange '%s': %w", exchangeName, err)
	}
	m.logger.WithField("exchange", exchangeName).Info("Exchange declared successfully")

	// Declare the queue as durable
	if err := m.DeclareQueue(queueName); err != nil {
		return fmt.Errorf("failed to declare queue '%s': %w", queueName, err)
	}
	m.logger.WithField("queue", queueName).Info("Queue declared successfully")

	// Bind the queue to the exchange
	if err := m.BindQueue(queueName, exchangeName, routingKey); err != nil {
		return fmt.Errorf("failed to bind queue '%s' to exchange '%s': %w", queueName, exchangeName, err)
	}
	m.logger.WithFields(logrus.Fields{
		"queue":       queueName,
		"exchange":    exchangeName,
		"routing_key": routingKey,
	}).Info("Queue bound to exchange successfully")

	return nil
}

func (m *Middleware) DeclareQueue(queueName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.channel.QueueDeclare(
		queueName, // name
		true,      // durable - la cola sobrevive al reinicio de RabbitMQ
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	return err
}

func (m *Middleware) DeclareExchange(exchangeName string, exchangeType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable - el exchange sobrevive al reinicio de RabbitMQ
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
}

func (m *Middleware) BindQueue(queueName, exchangeName, routingKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()

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
