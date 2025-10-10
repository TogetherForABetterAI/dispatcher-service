package middleware

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
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
		true,      // durable
		false,     // delete when unused
		false,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	return err
}

func (m *Middleware) DeclareExchange(exchangeName string, exchangeType string) error {
	return m.channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
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

func (m *Middleware) Publish(routingKey string, message []byte, exchangeName string) error {
	for attempt := 1; attempt <= m.MiddlewareConfig.GetMaxRetries(); attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := m.channel.PublishWithContext(
			ctx,
			exchangeName,
			routingKey,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Body:         message,
			},
		)
		cancel()

		if err != nil {
			m.logger.WithFields(logrus.Fields{
				"routing_key": routingKey,
				"exchange":    exchangeName,
			}).Error("Failed to publish message to exchange")
			continue
		}

		confirmed := <-m.confirms_chan

		if !confirmed.Ack {
			m.logger.WithFields(logrus.Fields{
				"routing_key": routingKey,
				"exchange":    exchangeName,
			}).Error("Failed to publish message to exchange")
			continue
		}

		m.logger.WithFields(logrus.Fields{
			"routing_key": routingKey,
			"exchange":    exchangeName,
		}).Debug("Published message to exchange")

		return nil
	}
	return fmt.Errorf("failed to publish message to exchange %s after %d attempts", exchangeName, m.MiddlewareConfig.GetMaxRetries())
}

func (m *Middleware) BasicConsume(queueName string, callback func(amqp.Delivery)) error {
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
		return err
	}

	go func() {
		for msg := range msgs {
			go func(m amqp.Delivery) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic in middleware callback: %v", r)
						_ = m.Nack(false, true)
					}
				}()
				callback(m)
			}(msg)
		}
	}()

	return nil
}

func (m *Middleware) Close() {
	if err := m.channel.Close(); err != nil {
		log.Printf("action: rabbitmq_channel_close | result: fail | error: %v", err)
	}
	if err := m.conn.Close(); err != nil {
		log.Printf("action: rabbitmq_connection_close | result: fail | error: %v", err)
	}
}

// Conn returns the underlying connection for reuse
func (m *Middleware) Conn() *amqp.Connection {
	return m.conn
}
