package common

import (
	"fmt"
	"net"
	"strings"
	"sync"

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
	shutdown    chan struct{}
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
		shutdown:    make(chan struct{}),
	}
}

func (rb *ResponseBuilder) Start() error {
	log.Info("Starting response builder")

	// Establish RabbitMQ connection with error handling
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

	// Set up client state management
	clients := &sync.Map{} // map[string]*clientState
	outChan := make(chan ResultMessage, 100)
	errChan := make(chan error, 4)

	for resultID := 1; resultID <= 4; resultID++ {
		queueName := fmt.Sprintf("results_%d_1", resultID)
		log.Infof("Listening on queue %s", queueName)

		go func(id int, qName string) {
			if err := rb.consumeResults(channel, qName, id, outChan, errChan); err != nil {
				log.Errorf("Consumer for queue %s failed: %v", qName, err)
			}
		}(resultID, queueName)
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

func (m *ResponseBuilder) consumeResults(
	channel *amqp.Channel,
	queueName string,
	resultID int,
	outChan chan ResultMessage,
	errChan chan error,
) error {
	resultsQueue := middleware.NewMessageMiddlewareQueue(queueName, channel)

	doneChan := make(chan error, 1)
	consumeChan := make(chan middleware.ConsumeChannel, 1)

	// Start consuming in separate goroutine
	go resultsQueue.StartConsuming(func(ch middleware.ConsumeChannel, done chan error) {
		consumeChan <- ch
		<-done // Block until done
	})

	// Wait for consume channel
	var msgChan middleware.ConsumeChannel
	select {
	case msgChan = <-consumeChan:
	}

	log.Infof("Results consumer started for queue %s", queueName)

	for {
		select {
		case msg, ok := <-*msgChan:
			if !ok {
				err := fmt.Errorf("consume channel closed for queue %s", queueName)
				errChan <- err
				return err
			}

			select {
			case outChan <- ResultMessage{
				Value: string(msg.Body),
				ID:    resultID,
			}:

			case err := <-doneChan:
				errChan <- fmt.Errorf("consumer %s done with error: %w", queueName, err)
				return err
			}
		}
	}
}

func (rb *ResponseBuilder) processResult(msg ResultMessage, clients *sync.Map, channel *amqp.Channel) error {
	// Parse message
	lines := strings.SplitN(msg.Value, "\n", 2)
	if len(lines) < 2 {
		return fmt.Errorf("invalid message format: expected clientId\\nmessage")
	}

	clientId := lines[0]
	message := lines[1]

	// Get or create client state
	stateInterface, _ := clients.LoadOrStore(clientId, &clientState{
		results:   make(map[int][]string),
		eofCounts: make(map[int]int),
	})
	state := stateInterface.(*clientState)

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

func (rb *ResponseBuilder) Shutdown() {
	log.Info("Initiating shutdown")
	close(rb.shutdown)
}
