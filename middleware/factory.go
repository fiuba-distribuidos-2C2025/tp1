package middleware

import (
	"sync"
	
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueFactory creates MessageMiddleware instances
type QueueFactory interface {
	CreateQueue(queueName string) MessageMiddleware
	SetChannel(channel *amqp.Channel)
}

// RealQueueFactory creates real RabbitMQ queues
type RealQueueFactory struct {
	channel *amqp.Channel
}

func NewRealQueueFactory(channel *amqp.Channel) *RealQueueFactory {
	return &RealQueueFactory{channel: channel}
}

func (f *RealQueueFactory) CreateQueue(queueName string) MessageMiddleware {
	return NewMessageMiddlewareQueue(queueName, f.channel)
}

func (f *RealQueueFactory) SetChannel(channel *amqp.Channel) {
	f.channel = channel
}

// MockQueueFactory creates mock queues for testing
type MockQueueFactory struct {
	queues map[string]MessageMiddleware
	mu     sync.RWMutex
}

func NewMockQueueFactory() *MockQueueFactory {
	return &MockQueueFactory{
		queues: make(map[string]MessageMiddleware),
	}
}

func (f *MockQueueFactory) CreateQueue(queueName string) MessageMiddleware {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Return existing queue if already created
	if queue, exists := f.queues[queueName]; exists {
		return queue
	}

	// Create new mock queue
	queue := NewMockMessageMiddleware(queueName)
	f.queues[queueName] = queue
	return queue
}

func (f *MockQueueFactory) SetChannel(channel *amqp.Channel) {
	// No-op for mock factory
}