package common

import (
	"net"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ResponseBuilderConfig struct {
	MiddlewareUrl string
}

type ResponseBuilder struct {
	Config      ResponseBuilderConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewResponseBuilder(config ResponseBuilderConfig) *ResponseBuilder {
	return &ResponseBuilder{
		Config:      config,
		listener:    nil,
		workerAddrs: nil,
		shutdown:    make(chan struct{}),
	}
}

func (m *ResponseBuilder) Start() error {
	rabbit_conn, _ := amqp.Dial(m.Config.MiddlewareUrl)
	channel, _ := rabbit_conn.Channel()
	resultsExchange1 := middleware.NewMessageMiddlewareExchange("results", []string{"query1"}, channel)
	outChan := make(chan string)
	resultsExchange1.StartConsuming(ResultsCallback(outChan))
	// resultsExchange2 := middleware.NewMessageMiddlewareExchange("results", []string{"query2"}, channel)
	// resultsExchange2.StartConsuming(ResultsCallback())
	// resultsExchange3 := middleware.NewMessageMiddlewareExchange("results", []string{"query3"}, channel)
	// resultsExchange3.StartConsuming(ResultsCallback())
	// resultsExchange4 := middleware.NewMessageMiddlewareExchange("results", []string{"query4"}, channel)
	// resultsExchange4.StartConsuming(ResultsCallback())
	//
	//
	select {
	case msg := <-outChan:
		channel, _ = rabbit_conn.Channel()
		log.Info("Resending query 1 result")
		finalResultsExchange := middleware.NewMessageMiddlewareExchange("final_results", []string{"query1"}, channel)
		finalResultsExchange.Send([]byte(msg))
	}

	return nil
}

func ResultsCallback(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for response...")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Info("Channel closed")
					return
				}
				outChan <- string(msg.Body)
				log.Infof("Message received - Length: %d, Body: %s", len(msg.Body), string(msg.Body))
				msg.Ack(false)
			}
		}
	}
}

// package common

// import (
// 	"fmt"

// 	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
// 	"github.com/op/go-logging"
// 	amqp "github.com/rabbitmq/amqp091-go"
// )

// var log = logging.MustGetLogger("log")

// const (
// 	resultsExchangeName      = "results"
// 	finalResultsExchangeName = "final_results"
// 	query1RoutingKey         = "query1"
// )

// // ResponseBuilderConfig holds configuration for the response builder
// type ResponseBuilderConfig struct {
// 	MiddlewareURL string
// }

// // ResponseBuilder processes query results and forwards them to final results
// type ResponseBuilder struct {
// 	config   ResponseBuilderConfig
// 	shutdown chan struct{}
// }

// // NewResponseBuilder creates a new ResponseBuilder instance
// func NewResponseBuilder(config ResponseBuilderConfig) *ResponseBuilder {
// 	return &ResponseBuilder{
// 		config:   config,
// 		shutdown: make(chan struct{}),
// 	}
// }

// // Start begins processing results from the results exchange
// func (rb *ResponseBuilder) Start() error {
// 	rabbitConn, err := amqp.Dial(rb.config.MiddlewareURL)
// 	if err != nil {
// 		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
// 	}
// 	defer rabbitConn.Close()

// 	log.Info("Processing query1")
// 	// Process query 1 results
// 	if err := rb.processQuery1Results(rabbitConn); err != nil {
// 		return fmt.Errorf("failed to process query 1 results: %w", err)
// 	}

// 	// TODO: Add support for additional queries
// 	// if err := rb.processQuery2Results(rabbitConn); err != nil {
// 	//     return fmt.Errorf("failed to process query 2 results: %w", err)
// 	// }

// 	return nil
// }

// // Stop gracefully shuts down the response builder
// func (rb *ResponseBuilder) Stop() {
// 	log.Info("Shutting down response builder...")
// 	close(rb.shutdown)
// 	log.Info("Response builder shutdown complete")
// }

// // processQuery1Results consumes query 1 results and forwards them to final results
// func (rb *ResponseBuilder) processQuery1Results(rabbitConn *amqp.Connection) error {
// 	// Create channel for consuming
// 	consumeChannel, err := rabbitConn.Channel()
// 	if err != nil {
// 		return fmt.Errorf("failed to open consume channel: %w", err)
// 	}
// 	defer consumeChannel.Close()

// 	// Set up results exchange consumer
// 	resultsExchange := middleware.NewMessageMiddlewareExchange(
// 		resultsExchangeName,
// 		[]string{query1RoutingKey},
// 		consumeChannel,
// 	)

// 	resultChan := make(chan string, 1)
// 	doneChan := make(chan error, 1)

// 	log.Info("Consuming")
// 	// Start consuming results
// 	resultsExchange.StartConsuming(createResultsCallback(resultChan, doneChan))
// 	log.Info("Waiting for query 1 results...")

// 	// Wait for result or error
// 	select {
// 	case result := <-resultChan:
// 		log.Infof("Query 1 result received - Length: %d", len(result))
// 		return rb.forwardToFinalResults(rabbitConn, result)
// 	case err := <-doneChan:
// 		return fmt.Errorf("consumer error: %w", err)
// 	case <-rb.shutdown:
// 		return fmt.Errorf("shutdown requested")
// 	}
// }

// // forwardToFinalResults sends the processed result to the final results exchange
// func (rb *ResponseBuilder) forwardToFinalResults(rabbitConn *amqp.Connection, result string) error {
// 	// Create new channel for sending
// 	sendChannel, err := rabbitConn.Channel()
// 	if err != nil {
// 		return fmt.Errorf("failed to open send channel: %w", err)
// 	}
// 	defer sendChannel.Close()

// 	log.Info("Forwarding query 1 result to final results")

// 	finalResultsExchange := middleware.NewMessageMiddlewareExchange(
// 		finalResultsExchangeName,
// 		[]string{query1RoutingKey},
// 		sendChannel,
// 	)

// 	finalResultsExchange.Send([]byte(result))
// 	log.Info("Query 1 result successfully forwarded to final results")
// 	return nil
// }

// // createResultsCallback creates a callback function for consuming results
// func createResultsCallback(resultChan chan string, doneChan chan error) func(middleware.ConsumeChannel, chan error) {
// 	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
// 		log.Info("Results consumer started")

// 		for {
// 			select {
// 			case msg, ok := <-*consumeChannel:
// 				if !ok {
// 					log.Info("Consume channel closed")
// 					doneChan <- fmt.Errorf("channel closed unexpectedly")
// 					return
// 				}

// 				log.Infof("Message received - Length: %d, Body: %s", len(msg.Body), string(msg.Body))

// 				if err := msg.Ack(false); err != nil {
// 					log.Errorf("Failed to acknowledge message: %v", err)
// 					doneChan <- fmt.Errorf("failed to ack message: %w", err)
// 					return
// 				}

// 				resultChan <- string(msg.Body)
// 				return

// 			case err := <-done:
// 				log.Errorf("Consumer error: %v", err)
// 				doneChan <- err
// 				return
// 			}
// 		}
// 	}
// }

// // TODO: Future query processors
// // func (rb *ResponseBuilder) processQuery2Results(rabbitConn *amqp.Connection) error {
// //     // Implementation for query 2
// // }
// //
// // func (rb *ResponseBuilder) processQuery3Results(rabbitConn *amqp.Connection) error {
// //     // Implementation for query 3
// // }
// //
// // func (rb *ResponseBuilder) processQuery4Results(rabbitConn *amqp.Connection) error {
// //     // Implementation for query 4
// // }
