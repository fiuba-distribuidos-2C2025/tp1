package common

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/healthcheck"
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
	IsTest                  bool
	BaseDir                 string
}

type ResponseBuilder struct {
	Config       ResponseBuilderConfig
	listener     net.Listener
	workerAddrs  []string
	shutdown     chan os.Signal
	queueFactory middleware.QueueFactory
}

type ResultMessage struct {
	ID    int
	Value string
}

type clientState struct {
	results   map[int][]string
	eofCounts map[int]int
}

func NewResponseBuilder(config ResponseBuilderConfig, factory middleware.QueueFactory) *ResponseBuilder {
	healthcheck.InitHealthChecker()
	return &ResponseBuilder{
		Config:       config,
		listener:     nil,
		workerAddrs:  nil,
		shutdown:     make(chan os.Signal, 1),
		queueFactory: factory,
	}
}

func (rb *ResponseBuilder) Start() error {
	log.Info("Starting response builder")

	if !rb.Config.IsTest {
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
		rb.queueFactory.SetChannel(channel)
	}

	clients := make(map[string]*clientState)
	// workaround to discard duplicate messages
	receivedMessages := make(map[string]map[string]map[string]bool)
	outChan := make(chan ResultMessage, 100)
	errChan := make(chan error, 4)

	// Start all consumers - StartConsuming already launches goroutines
	for resultID := 1; resultID <= 4; resultID++ {
		queueName := fmt.Sprintf("results_%d_1", resultID)
		log.Infof("Listening on queue %s", queueName)

		resultsQueue := rb.queueFactory.CreateQueue(queueName)
		resultsQueue.StartConsuming(resultsCallback(outChan, errChan, resultID, rb.Config.IsTest, rb.Config.BaseDir, queueName))
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
			if err := rb.processResult(msg, clients, receivedMessages); err != nil {
				log.Errorf("Failed to process result: %v", err)
			}
		}
	}
}

func (rb *ResponseBuilder) processResult(msg ResultMessage, clients map[string]*clientState, receivedMessages map[string]map[string]map[string]bool) error {
	// Parse message
	lines := strings.SplitN(msg.Value, "\n", 3)
	if len(lines) < 3 {
		return fmt.Errorf("invalid message format: expected clientId\\nmessage")
	}

	clientId := lines[0]
	msgID := lines[1]
	message := lines[2]

	// Check for duplicate messages
	if _, exists := receivedMessages[clientId]; !exists {
		receivedMessages[clientId] = make(map[string]map[string]bool)
	}
	if _, exists := receivedMessages[clientId][strconv.Itoa(msg.ID)]; !exists {
		receivedMessages[clientId][strconv.Itoa(msg.ID)] = make(map[string]bool)
	}
	if _, exists := receivedMessages[clientId][strconv.Itoa(msg.ID)][msgID]; exists {
		log.Infof("Duplicate message received for client %s query %d msg %s, ignoring", clientId, msg.ID, msgID)
		return nil
	}
	receivedMessages[clientId][strconv.Itoa(msg.ID)][msgID] = true

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
			finalResultsQueue := rb.queueFactory.CreateQueue(finalQueueName)

			finalResult := strings.Join(state.results[msg.ID], "\n")
			finalResultsQueue.Send([]byte(finalResult))

			// Remove stored messages
			removeResultsDir(rb.Config.BaseDir, fmt.Sprintf("results_%d_1", msg.ID), clientId)

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

func resultsCallback(resultChan chan ResultMessage, errChan chan error, resultID int, isTest bool, baseDir string, queueName string) func(middleware.ConsumeChannel, chan error) {
	// Check previous result messages before starting consumption of new ones.
	// This ensures that if the response builder restarts, it sends the previously
	// collected messages though the channel.
	err := loadPreviousMessages(resultChan, baseDir, queueName, resultID)
	if err != nil {
		log.Errorf("Error checking existing messagess: %v", err)
	}

	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Results consumer started for query %d", resultID)

		for msg := range *consumeChannel {
			if !isTest {
				storeResultMessage(baseDir, queueName, string(msg.Body))

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to acknowledge message: %v", err)
					errChan <- fmt.Errorf("failed to ack message: %w", err)
					continue
				}
			}
			// log.Debugf("Received message for query %d: %s", resultID, string(msg.Body)
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

// Reads previously received messages, and sends them through the channel.
func loadPreviousMessages(resultChan chan ResultMessage, baseDir string, queueName string, resultID int) error {
	resultsDir := filepath.Join(baseDir, queueName)

	// If directory doesn't exist, nothing to load — return nil (no error).
	if _, err := os.Stat(resultsDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		entryPath := filepath.Join(resultsDir, entry.Name())
		if entry.IsDir() {
			subEntries, err := os.ReadDir(entryPath)
			if err != nil {
				return err
			}
			for _, sub := range subEntries {
				// skip nested directories (if any)
				if sub.IsDir() {
					continue
				}
				filePath := filepath.Join(entryPath, sub.Name())
				data, err := os.ReadFile(filePath)
				if err != nil {
					return err
				}
				resultChan <- ResultMessage{
					Value: string(data),
					ID:    resultID,
				}
			}
			continue
		}
	}

	return nil
}

// Stores received message (appends if the file already exists).
func storeResultMessage(baseDir, queueName, body string) error {
	lines := strings.SplitN(body, "\n", 3)
	if len(lines) == 0 {
		return fmt.Errorf("body is empty, cannot extract clientId")
	}
	clientId := strings.TrimSpace(lines[0])
	msgId := strings.TrimSpace(lines[1])

	resultsDir := filepath.Join(baseDir, queueName, clientId)
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return err
	}

	// If any entry contains the substring msgId, remove it
	// as it is a duplicate message.
	// Prevents possibly contaminated entries
	for _, e := range entries {
		name := e.Name()
		if strings.Contains(name, msgId) {
			fullPath := filepath.Join(resultsDir, name)
			if err := os.Remove(fullPath); err != nil {
				return fmt.Errorf("failed to remove %s: %w", fullPath, err)
			}
		}
	}

	fileName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), msgId)
	path := filepath.Join(resultsDir, fileName)

	return os.WriteFile(path, []byte(body), 0o644)
}

func removeResultsDir(baseDir, queueName, clientId string) error {
	resultsDir := filepath.Join(baseDir, queueName, clientId)
	log.Debugf("Removing results directory %s", resultsDir)
	return os.RemoveAll(resultsDir)
}
