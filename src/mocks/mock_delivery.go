package mocks

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
)

// MockDelivery is a mock that wraps amqp.Delivery to intercept Ack/Nack calls
type MockDelivery struct {
	mock.Mock
	Body []byte
}

func (m *MockDelivery) Ack(tag uint64, multiple bool) error {
	args := m.Called(tag, multiple)
	return args.Error(0)
}

func (m *MockDelivery) Nack(tag uint64, multiple, requeue bool) error {
	args := m.Called(tag, multiple, requeue)
	return args.Error(0)
}

func (m *MockDelivery) Reject(tag uint64, requeue bool) error {
	args := m.Called(tag, requeue)
	return args.Error(0)
}

// ToDelivery converts MockDelivery to amqp.Delivery with custom Acknowledger
func (m *MockDelivery) ToDelivery() amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: m,
		Body:         m.Body,
		DeliveryTag:  1, // Use a fixed tag for testing
	}
}
