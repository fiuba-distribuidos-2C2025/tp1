package common

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"slices"
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
	resultTimeout       = 60 * time.Second
	maxScannerBuffer    = 128 * 1024 * 1024 // 128MB to handle 64MB chunks with overhead
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
	Config             RequestHandlerConfig
	listener           net.Listener
	shutdown           chan struct{}
	Channel            *amqp.Channel
	currentWorkerQueue int
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
		Config:             config,
		shutdown:           make(chan struct{}),
		currentWorkerQueue: 1,
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

	rabbit_conn, err := amqp.Dial(rh.Config.MiddlewareURL)
	if err != nil {
		return fmt.Errorf("failed to connect to AMQP server: %w", err)
	}

	rabbit_channel, err := rabbit_conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open AMQP channel: %w", err)
	}

	rh.Channel = rabbit_channel
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

	// Create a single scanner for the entire connection
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, maxScannerBuffer)
	scanner.Buffer(buf, maxScannerBuffer)
	filesProcessed := 0

	// Keep processing files until EOF is received
	for {
		fileProcessed, isEOF, err := rh.processFile(scanner, conn)

		if err != nil {
			if err == io.EOF {
				log.Infof("Client closed connection after %d files", filesProcessed)
				return
			}
			log.Errorf("Failed to process file: %v", err)
			return
		}

		if fileProcessed {
			filesProcessed++
			log.Infof("Successfully processed file %d", filesProcessed)
		}

		if isEOF {
			log.Infof("Received EOF from client after %d files", filesProcessed)

			// Send EOF to RabbitMQ queues
			if err := rh.sendEOF(); err != nil {
				log.Errorf("Failed to send EOF to queues: %v", err)
				return
			}

			// Send ACK for EOF
			if err := rh.sendACK(conn); err != nil {
				log.Errorf("Failed to send EOF ACK: %v", err)
				return
			}

			log.Info("Successfully processed all files")
			break
		}
	}

	resultsExchange := middleware.NewMessageMiddlewareQueue(
		"final_results_1",
		rh.Channel,
	)
	resultChan := make(chan string, 1)
	doneChan := make(chan error, 1)

	// Start consuming
	resultsExchange.StartConsuming(createResultsCallback(resultChan, doneChan))
	rh.waitForFinalResult(resultChan, doneChan, conn)
}

// processFile handles the transfer and processing of a single file
// Returns (fileProcessed, isEOF, error)
func (rh *RequestHandler) processFile(scanner *bufio.Scanner, conn net.Conn) (bool, bool, error) {
	var lastFileHash string
	var totalChunks int
	chunksReceived := 0

	log.Debug("Starting to process new file or EOF")

	// Keep reading messages until all chunks are received for this file
	for {
		log.Debug("Reading next message...")
		message, isEOF, err := rh.readMessage(scanner)
		if err != nil {
			log.Errorf("Error reading message: %v", err)
			return false, false, err
		}

		// Check if we received EOF marker
		if isEOF {
			log.Info("EOF marker detected")
			return false, true, nil
		}

		// Track file transfer progress
		if lastFileHash == "" {
			lastFileHash = message.FileHash
			totalChunks = message.TotalChunks
			log.Infof("Starting new file: %s with %d total chunks", lastFileHash, totalChunks)
		}

		// // If we receive a new file, we need to handle the previous file first
		// if message.FileHash != lastFileHash {
		// 	log.Errorf("Received chunk from different file. Expected: %s, Got: %s", lastFileHash, message.FileHash)
		// 	return false, false, fmt.Errorf("received chunks from multiple files simultaneously")
		// }

		chunksReceived++
		log.Infof("Received batch: %s (chunk %d/%d) with %d rows",
			message.FileHash, message.CurrentChunk, message.TotalChunks, len(message.CSVRows))

		// Process each message through RabbitMQ
		receiverID := rh.currentWorkerQueue
		if receiverID > rh.Config.ReceiversCount {
			rh.currentWorkerQueue = 1
		} else {
			rh.currentWorkerQueue++
		}

		if err := rh.sendToQueue(message, receiverID); err != nil {
			return false, false, fmt.Errorf("failed to send to queue: %w", err)
		}

		// Send ACK after successfully processing the chunk
		if err := rh.sendACK(conn); err != nil {
			return false, false, fmt.Errorf("failed to send ACK: %w", err)
		}
		log.Debugf("Sent ACK for chunk %d/%d", message.CurrentChunk, message.TotalChunks)

		// Check if all chunks have been received
		if chunksReceived >= totalChunks {
			log.Infof("All %d chunks received for file %s", chunksReceived, lastFileHash)
			return true, false, nil
		}
	}
}

// sendACK sends an acknowledgment message to the client
func (rh *RequestHandler) sendACK(conn net.Conn) error {
	writer := bufio.NewWriter(conn)

	if _, err := writer.WriteString("ACK\n"); err != nil {
		return fmt.Errorf("failed to write ACK: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush ACK: %w", err)
	}

	return nil
}

// readMessage reads and parses a message from the connection
// Returns (message, isEOF, error)
func (rh *RequestHandler) readMessage(scanner *bufio.Scanner) (*BatchMessage, bool, error) {
	// Read header
	log.Debug("Scanning for header...")
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Errorf("Scanner error: %v", err)
			return nil, false, err
		}
		log.Debug("Scanner reached EOF (no error)")
		return nil, false, io.EOF
	}

	headerText := scanner.Text()
	log.Debugf("Read header: %s", headerText)

	// Check if it's an EOF message
	if headerText == "EOF" {
		log.Info("Received EOF marker from client")
		// Read the empty line after EOF
		// if scanner.Scan() {
		// 	log.Debugf("Read line after EOF: '%s'", scanner.Text())
		// }
		return nil, true, nil
	}

	message, err := parseHeader(headerText)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse header: %w", err)
	}

	// Read CSV rows
	rowCount := 0
	for scanner.Scan() {
		row := scanner.Text()
		if row == "" {
			log.Debugf("Found empty line (end of message) after %d rows", rowCount)
			break
		}
		message.CSVRows = append(message.CSVRows, row)
		rowCount++
	}

	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("error reading connection: %w", err)
	}

	log.Debugf("Successfully read message with %d rows", len(message.CSVRows))
	return message, false, nil
}

// sendToQueue sends the batch message to the transactions queue
func (rh *RequestHandler) sendToQueue(message *BatchMessage, receiverID int) error {
	queue := middleware.NewMessageMiddlewareQueue("transactions"+"_"+strconv.Itoa(receiverID), rh.Channel)
	payload := strings.Join(message.CSVRows, "\n")
	queue.Send([]byte(payload))
	log.Infof("Successfully forwarded batch (chunk %d/%d) to queue transactions_%d",
		message.CurrentChunk, message.TotalChunks, receiverID)
	return nil
}

// sendEOF sends EOF message to all receiver queues
func (rh *RequestHandler) sendEOF() error {
	for i := 1; i <= rh.Config.ReceiversCount; i++ {
		queue := middleware.NewMessageMiddlewareQueue("transactions"+"_"+strconv.Itoa(i), rh.Channel)
		queue.Send([]byte("EOF"))
		log.Infof("Successfully sent EOF to transactions_%d", i)
	}
	return nil
}

// waitForFinalResult waits for the final processing result
func (rh *RequestHandler) waitForFinalResult(resultChan chan string, doneChan chan error, conn net.Conn) {
	select {
	case result := <-resultChan:
		// log.Infof("Result received - Length: %d - Message: %s", len(result), result)
		list := strings.Split(result, "\n")
		slices.Sort(list)
		finalResult := strings.Join(list, "\n")
		log.Infof("Result received - Length: %d - Message: %s", len(finalResult), finalResult)
		// Send result back to client
		if err := rh.sendResponse(conn, finalResult); err != nil {
			log.Errorf("Failed to send response: %v", err)
		}
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
			case err := <-done:
				doneChan <- err
			}
		}
	}
}
