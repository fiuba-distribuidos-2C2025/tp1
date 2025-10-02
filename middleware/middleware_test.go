package middleware

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkingQueueOneToOne(t *testing.T) {
	conn, ch := setupRabbitMQConnection()

	defer conn.Close()
	defer ch.Close()

	queueName := "test_queue_1to1"

	// Clean up any existing queue
	ch.QueueDelete(queueName, false, false, false)

	// Create sender and receiver
	sender := NewMessageMiddlewareQueue(queueName, ch)
	receiver := NewMessageMiddlewareQueue(queueName, ch)

	// Setup message collection
	var receivedMessages []string

	callback := createTestCallback(&receivedMessages)

	// Start consuming
	middlewareErr := receiver.StartConsuming(callback)

	// MessageMiddlewareError(0) means that no error occurred.
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to start consuming")

	// Send a message
	testMessage := []byte("Hello from sender")
	middlewareErr = sender.Send(testMessage)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message")

	// Wait a second for message to be received
	time.Sleep(1 * time.Second)

	// Verify correct message was received
	assert.Len(t, receivedMessages, 1, "Should receive exactly 1 message")
	assert.Equal(t, "Hello from sender", receivedMessages[0], "Message content mismatch")

	// Clean up
	receiver.StopConsuming()
	sender.Close()
	receiver.Close()
	ch.QueueDelete(queueName, false, false, false)
}

func TestWorkingQueueOneToOneTwice(t *testing.T) {
	conn, ch := setupRabbitMQConnection()

	defer conn.Close()
	defer ch.Close()

	queueName := "test_queue_1to1"

	// Clean up any existing queue
	ch.QueueDelete(queueName, false, false, false)

	// Create sender and receiver
	sender := NewMessageMiddlewareQueue(queueName, ch)
	receiver := NewMessageMiddlewareQueue(queueName, ch)

	// Setup message collection
	var receivedMessages []string

	callback := createTestCallback(&receivedMessages)

	// Start consuming
	middlewareErr := receiver.StartConsuming(callback)

	// MessageMiddlewareError(0) means that no error occurred.
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to start consuming")

	// Send a message
	testMessage := []byte("Hello from sender")
	middlewareErr = sender.Send(testMessage)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message")

	// Wait a second for message to be received
	time.Sleep(1 * time.Second)

	// Verify correct message was received
	assert.Len(t, receivedMessages, 1, "Should receive exactly 1 message")
	assert.Equal(t, "Hello from sender", receivedMessages[0], "Message content mismatch")

	// Send another message
	testMessage = []byte("Goodbye from sender")
	middlewareErr = sender.Send(testMessage)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message")

	// Wait a second for message to be received
	time.Sleep(1 * time.Second)

	// Verify correct message was received
	assert.Len(t, receivedMessages, 2, "Should receive exactly 2 message")
	assert.Equal(t, "Goodbye from sender", receivedMessages[1], "Message content mismatch")

	// Clean up
	receiver.StopConsuming()
	sender.Close()
	receiver.Close()
	ch.QueueDelete(queueName, false, false, false)
}

func TestWorkingQueueOneToMany(t *testing.T) {
	conn, ch := setupRabbitMQConnection()
	defer conn.Close()
	defer ch.Close()

	queueName := "test_queue_1toN"
	numReceivers := 3
	numMessages := 6

	// Clean up any existing queue
	ch.QueueDelete(queueName, false, false, false)

	// Create sender
	sender := NewMessageMiddlewareQueue(queueName, ch)

	// Create multiple receivers
	receivers := make([]*MessageMiddlewareQueue, numReceivers)
	receivedMessages := make([][]string, numReceivers)

	for i := 0; i < numReceivers; i++ {
		// Each receiver needs its own channel for proper load balancing
		newCh, err := conn.Channel()
		if err != nil {
			fmt.Printf("Error creating channel for receiver %d: %v\n", i, err)
			panic("Failed to create channel for receiver")
		}
		defer newCh.Close()

		receivers[i] = NewMessageMiddlewareQueue(queueName, newCh)
		receivedMessages[i] = make([]string, 0)

		callback := createTestCallbackForMultiple(&receivedMessages[i], 2)
		middlewareErr := receivers[i].StartConsuming(callback)
		assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to start consuming on receiver %d", i)
	}

	// Give consumers time to register
	time.Sleep(100 * time.Millisecond)

	// Send multiple messages
	for i := 0; i < numMessages; i++ {
		message := []byte(fmt.Sprintf("Message %d", i))
		middlewareErr := sender.Send(message)
		assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message %d", i)
	}

	// Wait for all messages to be processed
	time.Sleep(1 * time.Second)

	// Verify load balancing - messages should be distributed among receivers
	totalReceived := 0
	for i := 0; i < numReceivers; i++ {
		count := len(receivedMessages[i])
		totalReceived += count
		t.Logf("Receiver %d received %d messages", i, count)
	}

	assert.Equal(t, numMessages, totalReceived, "Total messages received should equal messages sent")

	// Clean up
	for i := 0; i < numReceivers; i++ {
		receivers[i].StopConsuming()
		receivers[i].Close()
	}
	sender.Close()
	ch.QueueDelete(queueName, false, false, false)
}

func TestExchangeOneToOne(t *testing.T) {
	conn, ch := setupRabbitMQConnection()
	defer conn.Close()
	defer ch.Close()

	exchangeName := "test_exchange_1to1"
	routingKey := "test.route.1to1"

	// Clean up any existing exchange
	ch.ExchangeDelete(exchangeName, false, false)

	// Create sender and receiver
	sender := NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, ch)
	receiver := NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, ch)

	// Setup message collection
	var receivedMessages []string
	callback := createTestCallback(&receivedMessages)

	// Start consuming
	middlewareErr := receiver.StartConsuming(callback)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to start consuming")

	// Give consumer time to bind
	time.Sleep(100 * time.Millisecond)

	// Send a message
	testMessage := []byte("Hello via exchange 1to1")
	middlewareErr = sender.Send(testMessage)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message")

	// Wait for message to be received
	time.Sleep(1 * time.Second)

	// Verify message was received
	assert.Len(t, receivedMessages, 1, "Should receive exactly 1 message")
	assert.Equal(t, "Hello via exchange 1to1", receivedMessages[0], "Message content mismatch")

	// Clean up
	receiver.StopConsuming()
	sender.Close()
	receiver.Close()
	ch.ExchangeDelete(exchangeName, false, false)
}

func TestExchangeOneToMany(t *testing.T) {
	conn, ch := setupRabbitMQConnection()
	defer conn.Close()
	defer ch.Close()

	exchangeName := "test_exchange_1toN"
	routingKey := "test.route.1toN"
	numReceivers := 3

	// Clean up any existing exchange
	ch.ExchangeDelete(exchangeName, false, false)

	// Create sender
	sender := NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, ch)

	// Create multiple receivers
	receivers := make([]*MessageMiddlewareExchange, numReceivers)
	receivedMessages := make([][]string, numReceivers)

	for i := 0; i < numReceivers; i++ {
		// Each receiver needs its own channel
		newCh, err := conn.Channel()
		if err != nil {
			fmt.Printf("Error creating channel for receiver %d: %v\n", i, err)
			panic("Failed to create channel for receiver")
		}
		defer newCh.Close()

		receivers[i] = NewMessageMiddlewareExchange(exchangeName, []string{routingKey}, newCh)
		receivedMessages[i] = make([]string, 0)

		callback := createTestCallback(&receivedMessages[i])
		middlewareErr := receivers[i].StartConsuming(callback)
		assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to start consuming on receiver %d", i)
	}

	// Give consumers time to bind
	time.Sleep(200 * time.Millisecond)

	// Send a message - in exchange pattern, all subscribers should receive it
	testMessage := []byte("Broadcast message to all subscribers")
	middlewareErr := sender.Send(testMessage)
	assert.Equal(t, MessageMiddlewareError(0), middlewareErr, "Failed to send message")

	time.Sleep(1 * time.Second)

	// Verify all receivers got the message (broadcast pattern)
	for i := 0; i < numReceivers; i++ {
		assert.Len(t, receivedMessages[i], 1, "Receiver %d should receive exactly 1 message", i)
		if len(receivedMessages[i]) > 0 {
			assert.Equal(t, "Broadcast message to all subscribers", receivedMessages[i][0], "Message content mismatch on receiver %d", i)
		}
	}

	// Clean up
	for i := 0; i < numReceivers; i++ {
		receivers[i].StopConsuming()
		receivers[i].Close()
	}
	sender.Close()
	ch.ExchangeDelete(exchangeName, false, false)
}
