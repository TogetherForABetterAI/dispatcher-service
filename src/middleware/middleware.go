// middleware.go
package middleware

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Middleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel 
}

func NewMiddleware() (*Middleware, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return nil, err
	}

	return &Middleware{
		conn:    conn,
		channel: ch,
	}, nil
}

func (m *Middleware) DeclareQueue(queueName string) error {
	_, err := m.channel.QueueDeclare(
		"",    	// name
		true,	// durable
		false, 	// delete when unused
		true,	// exclusive
		false, 	// no-wait
		nil,   	// arguments
	)
	return err
}

func (m *Middleware) DeclareExchange(exchangeName string, exchangeType string) error {
	return m.channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  	// durable
		false, 	// autoDelete
		false, 	// internal
		false, 	// noWait
		nil,	// arguments
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

func (m *Middleware) BasicSend(routingKey string, message []byte, exchangeName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()	
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
	if err != nil {
		log.Printf("action: message_sending | result: fail | error: %v | routing_key: %s\n", err, routingKey)
	}
	return err
}

func (m *Middleware) BasicConsume(queueName string, callback func(amqp.Delivery)) error {
	msgs, err := m.channel.Consume(
		queueName,
		"",    	// consumer
		false, 	// autoAck
		false, 	// exclusive
		false, 	// noLocal
		false, 	// noWait
		nil,	// args
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			func(m amqp.Delivery) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("action: rabbitmq_callback | result: fail | error: %v\n", r)
						_ = m.Nack(false, true)
					}
				}()
				callback(m)
				_ = m.Ack(false)
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
