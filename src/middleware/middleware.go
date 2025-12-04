package middleware

import (
	"fmt"
	"sync"
	"time"

	"github.com/dispatcher-service/src/config"
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
	Connect() error
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

	m := &Middleware{
		logger:           logger,
		MiddlewareConfig: cfg,
	}

	if err := m.Connect(); err != nil {
		return nil, err
	}

	return m, nil
}

// Connect establishes a connection to RabbitMQ with exponential backoff retry logic.
// It will keep retrying until successful, with delays increasing from 5s to a max of 60s.
func (m *Middleware) Connect() error {

	delay := config.InitialDelay
	maxDelay := config.MaxDelay

	for {
		url := fmt.Sprintf("amqp://%s:%s@%s:%d/",
			m.MiddlewareConfig.GetUsername(),
			m.MiddlewareConfig.GetPassword(),
			m.MiddlewareConfig.GetHost(),
			m.MiddlewareConfig.GetPort(),
		)

		conn, err := amqp.Dial(url)
		if err != nil {
			m.logger.WithFields(logrus.Fields{
				"host":     m.MiddlewareConfig.GetHost(),
				"port":     m.MiddlewareConfig.GetPort(),
				"error":    err.Error(),
				"retry_in": delay.String(),
			}).Error("Failed to connect to RabbitMQ. Retrying...")

			time.Sleep(delay)
			delay = min(delay*2, maxDelay) // exponential backoff
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			m.logger.WithFields(logrus.Fields{
				"error":    err.Error(),
				"retry_in": delay.String(),
			}).Error("Failed to open channel. Retrying...")

			time.Sleep(delay)
			delay = min(delay*2, maxDelay)
			continue
		}

		if err := ch.Confirm(false); err != nil {
			ch.Close()
			conn.Close()
			m.logger.WithFields(logrus.Fields{
				"error":    err.Error(),
				"retry_in": delay.String(),
			}).Error("Failed to enable confirms. Retrying...")

			time.Sleep(delay)
			delay = min(delay*2, maxDelay)
			continue
		}

		confirms_chan := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		if err := ch.Qos(1, 0, false); err != nil {
			ch.Close()
			conn.Close()
			m.logger.WithFields(logrus.Fields{
				"error":    err.Error(),
				"retry_in": delay.String(),
			}).Error("Failed to set QoS. Retrying...")

			time.Sleep(delay)
			delay = min(delay*2, maxDelay)
			continue
		}

		// Success - update the middleware state
		m.mu.Lock()
		m.conn = conn
		m.channel = ch
		m.confirms_chan = confirms_chan
		m.mu.Unlock()

		m.logger.WithFields(logrus.Fields{
			"host": m.MiddlewareConfig.GetHost(),
			"port": m.MiddlewareConfig.GetPort(),
			"user": m.MiddlewareConfig.GetUsername(),
		}).Info("Successfully connected to RabbitMQ")

		return nil
	}
}

// SetupTopology configures the RabbitMQ topology (exchange, queue, and binding)
func (m *Middleware) SetupTopology() error {
	exchangeName := config.DISPATCHER_EXCHANGE
	queueName := config.CONNECTION_QUEUE_NAME
	routingKey := queueName

	// Declare the exchange
	if err := m.DeclareExchange(exchangeName, "fanout"); err != nil {
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
