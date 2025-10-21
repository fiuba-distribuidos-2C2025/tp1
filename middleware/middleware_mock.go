package middleware

import (
	"fmt"
	"sync"
	amqp "github.com/rabbitmq/amqp091-go"
)

// MockMessageMiddleware implements the MessageMiddleware interface
type MockMessageMiddleware struct {
	queueName      string
	messages       [][]byte
	mu             sync.RWMutex
	consuming      bool
	consumeChan    chan amqp.Delivery
	stopChan       chan struct{}
	callbackDone   chan error
}

// NewMockMessageMiddleware creates a mock queue
func NewMockMessageMiddleware(queueName string) *MockMessageMiddleware {
	return &MockMessageMiddleware{
		queueName:    queueName,
		messages:     make([][]byte, 0),
		consumeChan:  make(chan amqp.Delivery, 100),
		stopChan:     make(chan struct{}),
		callbackDone: make(chan error, 1),
	}
}

func (m *MockMessageMiddleware) StartConsuming(callback onMessageCallback) MessageMiddlewareError {
	deliveryChan := (<-chan amqp.Delivery)(m.consumeChan)
	consumePtr := ConsumeChannel(&deliveryChan)
	
	go callback(consumePtr, m.callbackDone)

	existingMessages := make([][]byte, len(m.messages))
	copy(existingMessages, m.messages)

	go func() {
		for _, msg := range existingMessages {
			select {
			case m.consumeChan <- amqp.Delivery{Body: msg}:
			case <-m.stopChan:
				return
			}
		}
	}()

	return 0
}

func (m *MockMessageMiddleware) StopConsuming() MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.consuming {
		return 0
	}

	m.consuming = false
	close(m.stopChan)
	m.stopChan = make(chan struct{})
	
	return 0
}

func (m *MockMessageMiddleware) Send(message []byte) MessageMiddlewareError {
	fmt.Printf("Mock send to %s: %s\n", m.queueName, string(message))
	msgCopy := make([]byte, len(message))
	copy(msgCopy, message)
	m.messages = append(m.messages, msgCopy)

	return 0
}

func (m *MockMessageMiddleware) Close() MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.consuming {
		m.consuming = false
		close(m.consumeChan)
	}
	
	return 0
}

func (m *MockMessageMiddleware) Delete() MessageMiddlewareError {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = make([][]byte, 0)
	return 0
}

// Helper method for testing
func (m *MockMessageMiddleware) GetMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	messages := make([]string, len(m.messages))
	for i, msg := range m.messages {
		messages[i] = string(msg)
	}
	return messages
}