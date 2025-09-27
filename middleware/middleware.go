package middleware

import (
	"fmt"

	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Implementation for MessageMiddlewareQueue
func (q *MessageMiddlewareQueue) StartConsuming(m *MessageMiddlewareQueue, onMessageCallback onMessageCallback) MessageMiddlewareError {
	if q.channel == nil || (*amqp.Channel)(q.channel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	_, err := (*amqp.Channel)(q.channel).QueueDeclare(
		q.queueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Error declaring queue for consuming: %s", err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	msgs_chan, err := (*amqp.Channel)(q.channel).Consume(
		q.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Error consuming queue: %v", err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	q.consumeChannel = ConsumeChannel(&msgs_chan)

	done := make(chan error, 1)
	go onMessageCallback(q.consumeChannel, done)

	// Monitor for errors
	go func() {
		select {
		case err := <-done:
			if err != nil {
				// TODO: Handle error
				log.Printf("Callback error: %v\n", err)
			}
		}
	}()

	log.Printf("Starting consume on queue %s", q.queueName)
	return 0
}

func (q *MessageMiddlewareQueue) StopConsuming(m *MessageMiddlewareQueue) MessageMiddlewareError {
	if q.channel == nil || (*amqp.Channel)(q.channel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	if q.consumeChannel == nil {
		log.Println("Trying to stop consuming on innactive channel")
		return 0
	}

	// Cancel all consumers on this channel
	err := (*amqp.Channel)(q.channel).Cancel("", false)
	if err != nil {
		log.Printf("Error stopping consuming: %v\n", err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	log.Println("Consuming stopped with queue:", q.queueName)
	q.consumeChannel = nil
	return 0
}

func (q *MessageMiddlewareQueue) Send(m *MessageMiddlewareQueue, message []byte) MessageMiddlewareError {
	if q.channel == nil || (*amqp.Channel)(q.channel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	err := (*amqp.Channel)(q.channel).Publish(
		"",
		q.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		})

	if err != nil {
		log.Printf("Error sending message: %v\n", err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	log.Println("Message sent successfully")
	return 0 // Success
}

func (q *MessageMiddlewareQueue) Close(m *MessageMiddlewareQueue) MessageMiddlewareError {
	if q.channel == nil {
		log.Printf("Queue %s is already closed", q.queueName)
		return 0 // Already closed
	}

	err := (*amqp.Channel)(q.channel).Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}

	log.Printf("Queue %s closed successfully", q.queueName)
	q.channel = nil
	q.consumeChannel = nil
	return 0 // Success
}

func (q *MessageMiddlewareQueue) Delete(m *MessageMiddlewareQueue) MessageMiddlewareError {
	if q.channel == nil || (*amqp.Channel)(q.channel).IsClosed() {
		return MessageMiddlewareDeleteError
	}

	_, err := (*amqp.Channel)(q.channel).QueueDelete(
		q.queueName,
		false,
		false,
		false,
	)

	if err != nil {
		log.Printf("Failed to delete queue %s: %v", q.queueName, err)
		return MessageMiddlewareDeleteError
	}

	log.Printf("Queue %s deleted successfully", q.queueName)
	return 0
}

// Implementation for MessageMiddlewareExchange
func (e *MessageMiddlewareExchange) StartConsuming(m *MessageMiddlewareExchange, onMessageCallback onMessageCallback) MessageMiddlewareError {
	if e.amqpChannel == nil || (*amqp.Channel)(e.amqpChannel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	err := (*amqp.Channel)(e.amqpChannel).ExchangeDeclare(
		e.exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to declare exchange %s: %v", e.exchangeName, err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	q, err := (*amqp.Channel)(e.amqpChannel).QueueDeclare(
		"", // empty = generate random name
		false,
		true,
		true,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	// Bind the queue to the exchange with all routing keys
	for _, routeKey := range e.routeKeys {
		err = (*amqp.Channel)(e.amqpChannel).QueueBind(
			q.Name,         // queue name
			routeKey,       // routing key
			e.exchangeName, // exchange
			false,
			nil,
		)
		if err != nil {
			log.Printf("Failed to bind queue %s to exchange %s with routing key %s: %v", q.Name, e.exchangeName, routeKey, err)
			if isConnectionError(err) {
				return MessageMiddlewareDisconnectedError
			}
			return MessageMiddlewareMessageError
		}
	}

	msgs, err := (*amqp.Channel)(e.amqpChannel).Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Failed to consume messages from queue %s: %v", q.Name, err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
		return MessageMiddlewareMessageError
	}

	e.consumeChannel = ConsumeChannel(&msgs)

	// Start the callback in a goroutine
	done := make(chan error, 1)
	go onMessageCallback(e.consumeChannel, done)

	// Monitor for errors
	go func() {
		select {
		case err := <-done:
			if err != nil {
				// TODO: Handle callback errors
				fmt.Printf("Callback error: %v\n", err)
			}
		}
	}()

	log.Printf("Started consuming messages from queue %s", q.Name)
	return 0 // Success
}

func (e *MessageMiddlewareExchange) StopConsuming(m *MessageMiddlewareExchange) MessageMiddlewareError {
	if e.amqpChannel == nil || (*amqp.Channel)(e.amqpChannel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	if e.consumeChannel == nil {
		log.Println("Trying to stop consuming on innactive exchange")
		return 0
	}

	err := (*amqp.Channel)(e.amqpChannel).Cancel("", false)
	if err != nil {
		log.Printf("Error stopping consuming on exchange %s: %v", e.exchangeName, err)
		if isConnectionError(err) {
			return MessageMiddlewareDisconnectedError
		}
	}

	log.Printf("Stopped consuming messages from exchange %s", e.exchangeName)
	e.consumeChannel = nil
	return 0 // Success
}

func (e *MessageMiddlewareExchange) Send(m *MessageMiddlewareExchange, message []byte) MessageMiddlewareError {
	if e.amqpChannel == nil || (*amqp.Channel)(e.amqpChannel).IsClosed() {
		return MessageMiddlewareDisconnectedError
	}

	// Send to all routing keys
	for _, routeKey := range e.routeKeys {
		err := (*amqp.Channel)(e.amqpChannel).Publish(
			e.exchangeName,
			routeKey,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/octet-stream",
				Body:         message,
				DeliveryMode: amqp.Persistent,
			})

		if err != nil {
			log.Printf("Error sending message to exchange %s with routing key %s: %v", e.exchangeName, routeKey, err)
			if isConnectionError(err) {
				return MessageMiddlewareDisconnectedError
			}
			return MessageMiddlewareMessageError
		}
	}

	log.Printf("Sent message to exchange %s with routing keys %v", e.exchangeName, e.routeKeys)
	return 0
}

func (e *MessageMiddlewareExchange) Close(m *MessageMiddlewareExchange) MessageMiddlewareError {
	if e.amqpChannel == nil {
		log.Println("Trying to close exchange that is already closed")
		return 0
	}

	err := (*amqp.Channel)(e.amqpChannel).Close()
	if err != nil {
		return MessageMiddlewareCloseError
	}

	log.Printf("Exchange %s closed successfully", e.exchangeName)
	e.amqpChannel = nil
	e.consumeChannel = nil
	return 0
}

func (e *MessageMiddlewareExchange) Delete(m *MessageMiddlewareExchange) MessageMiddlewareError {
	if e.amqpChannel == nil || (*amqp.Channel)(e.amqpChannel).IsClosed() {
		return MessageMiddlewareDeleteError
	}

	err := (*amqp.Channel)(e.amqpChannel).ExchangeDelete(
		e.exchangeName,
		false,
		false,
	)

	if err != nil {
		log.Printf("Failed to delete exchange %s: %v", e.exchangeName, err)
		return MessageMiddlewareDeleteError
	}

	log.Printf("Exchange %s deleted successfully", e.exchangeName)
	return 0
}

// Constructor functions for easier initialization of the MessageMiddlewareQueue
func NewMessageMiddlewareQueue(queueName string, channel *amqp.Channel) *MessageMiddlewareQueue {
	return &MessageMiddlewareQueue{
		queueName: queueName,
		channel:   MiddlewareChannel(channel),
	}
}

// Constructor functions for easier initialization of the NewMessageMiddlewareExchange
func NewMessageMiddlewareExchange(exchangeName string, routeKeys []string, channel *amqp.Channel) *MessageMiddlewareExchange {
	return &MessageMiddlewareExchange{
		exchangeName: exchangeName,
		routeKeys:    routeKeys,
		amqpChannel:  MiddlewareChannel(channel),
	}
}
