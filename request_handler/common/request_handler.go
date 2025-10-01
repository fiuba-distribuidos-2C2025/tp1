package common

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const (
	messageTypeTransfer = "TRANSFER"
	headerPartsCount    = 5
	resultTimeout       = 30 * time.Second
)

// RequestHandlerConfig holds configuration for the request handler
type RequestHandlerConfig struct {
	Port           string
	IP             string
	MiddlewareURL  string
	ReceiversCount int
}

// RequestHandler handles incoming client connections and manages message flow
type RequestHandler struct {
	Config   RequestHandlerConfig
	listener net.Listener
	shutdown chan struct{}
}

// BatchMessage represents a parsed batch message from the client
type BatchMessage struct {
	FileType     string
	FileHash     string
	CurrentChunk int
	TotalChunks  int
	CSVRows      []string
}

// NewRequestHandler creates a new RequestHandler instance
func NewRequestHandler(config RequestHandlerConfig) *RequestHandler {
	return &RequestHandler{
		Config:   config,
		shutdown: make(chan struct{}),
	}
}

// Start begins listening for connections
func (rh *RequestHandler) Start() error {
	addr := net.JoinHostPort(rh.Config.IP, rh.Config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	rh.listener = listener
	log.Infof("RequestHandler listening on %s", addr)

	go rh.acceptConnections()

	<-rh.shutdown
	return nil
}

// Stop gracefully shuts down the request handler
func (rh *RequestHandler) Stop() {
	log.Info("Shutting down request handler...")
	close(rh.shutdown)

	if rh.listener != nil {
		if err := rh.listener.Close(); err != nil {
			log.Errorf("Error closing listener: %v", err)
		}
	}

	log.Info("RequestHandler shutdown complete")
}

// acceptConnections continuously accepts new connections
func (rh *RequestHandler) acceptConnections() {
	for {
		conn, err := rh.listener.Accept()
		if err != nil {
			select {
			case <-rh.shutdown:
				return
			default:
				log.Errorf("Error accepting connection: %v", err)
				continue
			}
		}

		go rh.handleConnection(conn)
	}
}

// handleConnection processes a single client connection
func (rh *RequestHandler) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Infof("New connection from %s", conn.RemoteAddr())

	// Read and parse the message
	message, err := rh.readMessage(conn)
	if err != nil {
		log.Errorf("Failed to read message: %v", err)
		return
	}

	log.Infof("Received batch: %s (chunk %d/%d) with %d rows",
		message.FileHash, message.CurrentChunk, message.TotalChunks, len(message.CSVRows))

	// Process the message through RabbitMQ
	result, err := rh.processMessage(message)
	if err != nil {
		log.Errorf("Failed to process message: %v", err)
		return
	}

	// Send result back to client
	if err := rh.sendResponse(conn, result); err != nil {
		log.Errorf("Failed to send response: %v", err)
		return
	}

	log.Infof("Successfully processed and responded to client")
}

// readMessage reads and parses a message from the connection
func (rh *RequestHandler) readMessage(conn net.Conn) (*BatchMessage, error) {
	scanner := bufio.NewScanner(conn)

	// Read header
	if !scanner.Scan() {
		return nil, fmt.Errorf("failed to read header")
	}

	message, err := parseHeader(scanner.Text())
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", err)
	}

	// Read CSV rows
	for scanner.Scan() {
		row := scanner.Text()
		if row == "" {
			break
		}
		message.CSVRows = append(message.CSVRows, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading connection: %w", err)
	}

	return message, nil
}

// processMessage sends the message to RabbitMQ and waits for a result
func (rh *RequestHandler) processMessage(message *BatchMessage) (string, error) {
	// Connect to RabbitMQ
	rabbitConn, err := amqp.Dial(rh.Config.MiddlewareURL)
	if err != nil {
		return "", fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer rabbitConn.Close()

	// Create channel
	channel, err := rabbitConn.Channel()
	if err != nil {
		return "", fmt.Errorf("failed to open channel: %w", err)
	}
	defer channel.Close()

	resultsExchange := middleware.NewMessageMiddlewareQueue(
		"final_results_1",
		channel,
	)
	resultChan := make(chan string, 1)
	doneChan := make(chan error, 1)
	// Start consuming
	resultsExchange.StartConsuming(createResultsCallback(resultChan, doneChan))
	receiverID := (message.CurrentChunk % rh.Config.ReceiversCount)
	if receiverID == 0 {
		receiverID = 1
	}

	// Send message to transactions queue
	if err := rh.sendToQueue(channel, message, receiverID); err != nil {
		return "", fmt.Errorf("failed to send to queue: %w", err)
	}

	if err := rh.sendEOF(channel); err != nil {
		return "", fmt.Errorf("failed to send EOF: %w", err)
	}

	// Wait for result
	result, err := rh.waitForResult(resultChan, doneChan)
	if err != nil {
		return "", fmt.Errorf("failed to receive result: %w", err)
	}

	return result, nil
}

// sendToQueue sends the batch message to the transactions queue
func (rh *RequestHandler) sendToQueue(channel *amqp.Channel, message *BatchMessage, receiverID int) error {
	queue := middleware.NewMessageMiddlewareQueue("transactions"+"_"+strconv.Itoa(receiverID), channel)
	payload := strings.Join(message.CSVRows, "\n")
	queue.Send([]byte(payload))
	log.Infof("Successfully forwarded batch to queue")
	return nil
}

func (rh *RequestHandler) sendEOF(channel *amqp.Channel) error {
	for i := 0; i < rh.Config.ReceiversCount; i++ {
		queue := middleware.NewMessageMiddlewareQueue("transactions"+"_"+strconv.Itoa(i), channel)
		queue.Send([]byte("EOF"))
		log.Infof("Successfully sent EOF")
	}
	return nil
}

// waitForResult waits for a result from the final_results exchange
func (rh *RequestHandler) waitForResult(resultChan chan string, doneChan chan error) (string, error) {
	// Wait for result with timeout
	ctx, cancel := context.WithTimeout(context.Background(), resultTimeout)
	defer cancel()

	select {
	case result := <-resultChan:
		log.Infof("Result received - Length: %d - Message: %s", len(result), result)
		return result, nil
	case err := <-doneChan:
		return "", fmt.Errorf("consumer error: %w", err)
	case <-ctx.Done():
		return "", fmt.Errorf("timeout waiting for result")
	}
}

// sendResponse writes the result back to the client
func (rh *RequestHandler) sendResponse(conn net.Conn, result string) error {
	writer := bufio.NewWriter(conn)

	if _, err := writer.WriteString(result); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	if _, err := writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write end marker: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush response: %w", err)
	}

	return nil
}

// parseHeader parses the message header
func parseHeader(header string) (*BatchMessage, error) {
	parts := strings.Split(header, ";")
	if len(parts) != headerPartsCount {
		return nil, fmt.Errorf("invalid header format: expected %d parts, got %d",
			headerPartsCount, len(parts))
	}

	if parts[0] != messageTypeTransfer {
		return nil, fmt.Errorf("invalid message type: expected %s, got %s",
			messageTypeTransfer, parts[0])
	}

	var currentChunk, totalChunks int
	if _, err := fmt.Sscanf(parts[3], "%d", &currentChunk); err != nil {
		return nil, fmt.Errorf("invalid current chunk: %w", err)
	}
	if _, err := fmt.Sscanf(parts[4], "%d", &totalChunks); err != nil {
		return nil, fmt.Errorf("invalid total chunks: %w", err)
	}

	return &BatchMessage{
		FileType:     parts[1],
		FileHash:     parts[2],
		CurrentChunk: currentChunk,
		TotalChunks:  totalChunks,
		CSVRows:      make([]string, 0),
	}, nil
}

// createResultsCallback creates a callback function for consuming results
func createResultsCallback(resultChan chan string, doneChan chan error) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for response...")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Info("Channel closed")
					doneChan <- fmt.Errorf("channel closed unexpectedly")
					return
				}

				log.Infof("Message received - Length: %d", len(msg.Body))

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to ack message: %v", err)
				}

				resultChan <- string(msg.Body)
				return
			case err := <-done:
				doneChan <- err
				return
			}
		}
	}
}
