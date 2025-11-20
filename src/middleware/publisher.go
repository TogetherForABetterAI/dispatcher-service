package middleware

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Publisher represents a RabbitMQ publisher with its own channel
type Publisher struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	confirms_chan chan amqp.Confirmation
	logger        *logrus.Logger
}

// NewPublisher creates a new publisher using an existing connection
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	confirms_chan := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.Qos(1, 0, false); err != nil {
		return nil, err
	}

	return &Publisher{
		conn:          conn,
		channel:       ch,
		confirms_chan: confirms_chan,
		logger:        logger,
	}, nil
}

// When you want a single message to be delivered to a single queue,
// you can publish to the default exchange with the routingKey of the queue name.
// This is because every declared queue gets an implicit route to the default exchange.
func (p *Publisher) Publish(routingKey string, message []byte, exchangeName string) error {
	for attempt := 1; attempt <= 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := p.channel.PublishWithContext(
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
			p.logger.WithFields(logrus.Fields{
				"routing_key": routingKey,
				"exchange":    exchangeName,
			}).Error("Failed to publish message to exchange")
			continue
		}

		confirmed := <-p.confirms_chan

		if !confirmed.Ack {
			p.logger.WithFields(logrus.Fields{
				"routing_key": routingKey,
				"exchange":    exchangeName,
			}).Error("Failed to publish message to exchange")
			continue
		}

		p.logger.WithFields(logrus.Fields{
			"routing_key": routingKey,
			"exchange":    exchangeName,
		}).Debug("Published message to exchange")

		return nil
	}
	return fmt.Errorf("failed to publish message to exchange %s after 5 attempts", exchangeName)
}

// Close closes only the channel, not the connection
func (p *Publisher) Close() {
	if err := p.channel.Close(); err != nil {
		log.Printf("action: rabbitmq_channel_close | result: fail | error: %v", err)
	}
}
