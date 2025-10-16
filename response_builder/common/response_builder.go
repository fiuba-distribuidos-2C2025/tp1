package common

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const EOF_MESSAGE_MAX_LENGTH = 16

type ResponseBuilderConfig struct {
	MiddlewareUrl           string
	WorkerResultsOneCount   int
	WorkerResultsTwoCount   int
	WorkerResultsThreeCount int
	WorkerResultsFourCount  int
}

type ResponseBuilder struct {
	Config      ResponseBuilderConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan os.Signal
}

type ResultMessage struct {
	ID    int
	Value string
}

type clientState struct {
	results   map[int][]string
	eofCounts map[int]int
}

func NewResponseBuilder(config ResponseBuilderConfig) *ResponseBuilder {
	return &ResponseBuilder{
		Config:      config,
		listener:    nil,
		workerAddrs: nil,
		shutdown:    make(chan os.Signal, 1),
	}
}

func (rb *ResponseBuilder) Start() error {
	log.Info("Starting response builder")

	conn, err := amqp.Dial(rb.Config.MiddlewareUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	clients := make(map[string]*clientState)
	outChan := make(chan ResultMessage, 100)
	errChan := make(chan error, 4)

	// Start all consumers - StartConsuming already launches goroutines
	for resultID := 1; resultID <= 4; resultID++ {
		queueName := fmt.Sprintf("results_%d_1", resultID)
		log.Infof("Listening on queue %s", queueName)

		resultsQueue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		resultsQueue.StartConsuming(resultsCallback(outChan, errChan, resultID))
	}

	// Main processing loop
	for {
		select {
		case <-rb.shutdown:
			log.Info("Shutdown signal received")
			return nil

		case err := <-errChan:
			log.Errorf("Consumer error: %v", err)
			// Continue processing, individual consumers handle their own errors

		case msg := <-outChan:
			if err := rb.processResult(msg, clients, channel); err != nil {
				log.Errorf("Failed to process result: %v", err)
			}
		}
	}
}

func (rb *ResponseBuilder) processResult(msg ResultMessage, clients map[string]*clientState, channel *amqp.Channel) error {
	// Parse message
	lines := strings.SplitN(msg.Value, "\n", 2)
	if len(lines) < 2 {
		return fmt.Errorf("invalid message format: expected clientId\\nmessage")
	}

	clientId := lines[0]
	message := lines[1]

	// Get or create client state
	state, ok := clients[clientId]
	if !ok {
		state = &clientState{
			results:   make(map[int][]string),
			eofCounts: make(map[int]int),
		}
		clients[clientId] = state
	}

	// Handle EOF messages
	if strings.Contains(message, "EOF") {
		log.Infof("EOF received from client %s for query %d", clientId, msg.ID)

		expectedEof := rb.getExpectedEofCount(msg.ID)
		state.eofCounts[msg.ID]++
		totalEOFs := state.eofCounts[msg.ID]

		log.Infof("Client %s query %d: %d/%d EOFs received", clientId, msg.ID, totalEOFs, expectedEof)

		if totalEOFs == expectedEof {
			log.Infof("All EOFs received for client %s query %d, sending final result", clientId, msg.ID)

			// Send final results
			finalQueueName := fmt.Sprintf("final_results_%s_%d", clientId, msg.ID)
			finalResultsQueue := middleware.NewMessageMiddlewareQueue(finalQueueName, channel)

			finalResult := strings.Join(state.results[msg.ID], "\n")
			finalResultsQueue.Send([]byte(finalResult))

			log.Infof("Successfully sent final results for client %s query %d", clientId, msg.ID)
		} else if totalEOFs > expectedEof {
			log.Warningf("Received more EOFs than expected for client %s query %d: %d > %d",
				clientId, msg.ID, totalEOFs, expectedEof)
		}

		return nil
	}

	// Accumulate regular messages
	log.Debugf("Result received from query %d for client %s", msg.ID, clientId)
	state.results[msg.ID] = append(state.results[msg.ID], message)

	return nil
}

func (rb *ResponseBuilder) getExpectedEofCount(queryID int) int {
	switch queryID {
	case 1:
		return rb.Config.WorkerResultsOneCount
	case 2:
		return rb.Config.WorkerResultsTwoCount
	case 3:
		return rb.Config.WorkerResultsThreeCount
	case 4:
		return rb.Config.WorkerResultsFourCount
	default:
		log.Warningf("Unknown query ID: %d, defaulting to 1 EOF", queryID)
		return 1
	}
}

func resultsCallback(resultChan chan ResultMessage, errChan chan error, resultID int) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Results consumer started for query %d", resultID)

		for msg := range *consumeChannel {
			if err := msg.Ack(false); err != nil {
				log.Errorf("Failed to acknowledge message: %v", err)
				errChan <- fmt.Errorf("failed to ack message: %w", err)
				continue
			}

			resultChan <- ResultMessage{
				Value: string(msg.Body),
				ID:    resultID,
			}
		}

		// Channel closed
		log.Warningf("Consume channel closed for query %d", resultID)
		errChan <- fmt.Errorf("consume channel closed for query %d", resultID)
	}
}

func (rb *ResponseBuilder) Shutdown() {
	log.Info("Initiating shutdown")
	rb.shutdown <- os.Interrupt
	close(rb.shutdown)
}
