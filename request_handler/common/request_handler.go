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
	resultChunkSize     = 10 * 1024 * 1024  // 10MB chunks for results
)

// ResultMessage contains a result and which queue it came from
type ResultMessage struct {
	QueueID int
	Data    string
}

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

	// Keep processing files until FINAL_EOF is received
	for {
		fileProcessed, isFileTypeEOF, isFinalEOF, fileType, err := rh.processFile(scanner, conn)

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

		// Handle EOF for specific fileType
		if isFileTypeEOF {
			log.Infof("Received EOF for fileType %d after %d files", fileType, filesProcessed)

			// Send EOF to the appropriate RabbitMQ queues for this fileType
			if err := rh.sendEOFForFileType(fileType); err != nil {
				log.Errorf("Failed to send EOF for fileType %d to queues: %v", fileType, err)
				return
			}

			// Send ACK for this fileType EOF
			if err := rh.sendACK(conn); err != nil {
				log.Errorf("Failed to send EOF ACK for fileType %d: %v", fileType, err)
				return
			}

			log.Infof("Successfully sent EOF for fileType %d to queues", fileType)
			continue
		}

		// Handle FINAL_EOF
		if isFinalEOF {
			log.Infof("Received FINAL_EOF from client after %d files", filesProcessed)

			// Send ACK for FINAL_EOF
			if err := rh.sendACK(conn); err != nil {
				log.Errorf("Failed to send FINAL_EOF ACK: %v", err)
				return
			}

			log.Info("Successfully processed all files and received FINAL_EOF")
			break
		}
	}

	// Listen to multiple final result queues
	resultChan := make(chan ResultMessage, 4)
	doneChan := make(chan error, 1)

	// Start consuming from all final result queues
	for i := 1; i <= 4; i++ {
		queueName := fmt.Sprintf("final_results_%d", i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		go queue.StartConsuming(createResultsCallback(resultChan, doneChan, i))
		log.Infof("Started consuming from %s", queueName)
	}

	rh.waitForFinalResults(resultChan, doneChan, conn)
}

// processFile handles the transfer and processing of a single file
// Returns (fileProcessed, isFileTypeEOF, isFinalEOF, fileType, error)
func (rh *RequestHandler) processFile(scanner *bufio.Scanner, conn net.Conn) (bool, bool, bool, int, error) {
	var lastFileHash string
	var totalChunks int
	chunksReceived := 0

	log.Debug("Starting to process new file or EOF")

	// Keep reading messages until all chunks are received for this file
	for {
		log.Debug("Reading next message...")
		message, isFileTypeEOF, isFinalEOF, fileType, err := rh.readMessage(scanner)
		if err != nil {
			log.Errorf("Error reading message: %v", err)
			return false, false, false, 0, err
		}

		// Check if we received FINAL_EOF marker
		if isFinalEOF {
			log.Info("FINAL_EOF marker detected")
			return false, false, true, 0, nil
		}

		// Check if we received EOF for a specific fileType
		if isFileTypeEOF {
			log.Infof("FileType EOF marker detected for fileType %d", fileType)
			return false, true, false, fileType, nil
		}

		// Track file transfer progress
		if lastFileHash == "" {
			lastFileHash = message.FileHash
			totalChunks = message.TotalChunks
			log.Infof("Starting new file: %s with %d total chunks", lastFileHash, totalChunks)
		}

		chunksReceived++
		log.Infof("Received batch: %s (chunk %d/%d) with %d rows",
			message.FileHash, message.CurrentChunk, message.TotalChunks, len(message.CSVRows))

		if message.FileType == "2" {
		}

		// Process each message through RabbitMQ
		receiverID := rh.currentWorkerQueue
		if receiverID > rh.Config.ReceiversCount {
			rh.currentWorkerQueue = 1
			receiverID = rh.currentWorkerQueue
		}

		if err := rh.sendToQueue(message, receiverID, message.FileType); err != nil {
			return false, false, false, 0, fmt.Errorf("failed to send to queue: %w", err)
		}

		rh.currentWorkerQueue++

		// Send ACK after successfully processing the chunk
		if err := rh.sendACK(conn); err != nil {
			return false, false, false, 0, fmt.Errorf("failed to send ACK: %w", err)
		}
		log.Debugf("Sent ACK for chunk %d/%d", message.CurrentChunk, message.TotalChunks)

		// Check if all chunks have been received
		if chunksReceived >= totalChunks {
			log.Infof("All %d chunks received for file %s", chunksReceived, lastFileHash)
			return true, false, false, 0, nil
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
// Returns (message, isFileTypeEOF, isFinalEOF, fileType, error)
func (rh *RequestHandler) readMessage(scanner *bufio.Scanner) (*BatchMessage, bool, bool, int, error) {
	// Read header
	log.Debug("Scanning for header...")
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Errorf("Scanner error: %v", err)
			return nil, false, false, 0, err
		}
		log.Debug("Scanner reached EOF (no error)")
		return nil, false, false, 0, io.EOF
	}

	headerText := scanner.Text()
	log.Debugf("Read header: %s", headerText)

	// Check if it's a FINAL_EOF message
	if headerText == "FINAL_EOF" {
		log.Info("Received FINAL_EOF marker from client")
		return nil, false, true, 0, nil
	}

	// Check if it's an EOF for a specific file type: EOF;<fileType>
	if strings.HasPrefix(headerText, "EOF;") {
		parts := strings.Split(headerText, ";")
		if len(parts) == 2 {
			fileType, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, false, false, 0, fmt.Errorf("invalid fileType in EOF: %w", err)
			}
			log.Infof("Received EOF for fileType %d from client", fileType)
			return nil, true, false, fileType, nil
		}
	}

	message, err := parseHeader(headerText)
	if err != nil {
		return nil, false, false, 0, fmt.Errorf("failed to parse header: %w", err)
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
		return nil, false, false, 0, fmt.Errorf("error reading connection: %w", err)
	}

	log.Debugf("Successfully read message with %d rows", len(message.CSVRows))
	return message, false, false, 0, nil
}

// sendToQueue sends the batch message to the transactions queue
func (rh *RequestHandler) sendToQueue(message *BatchMessage, receiverID int, fileType string) error {
	fileTypeInt, _ := strconv.Atoi(fileType)
	switch fileTypeInt {
	case 0:
		queue := middleware.NewMessageMiddlewareQueue("transactions"+"_"+strconv.Itoa(receiverID), rh.Channel)
		payload := strings.Join(message.CSVRows, "\n")
		queue.Send([]byte(payload))
		log.Infof("Successfully forwarded batch (chunk %d/%d) to queue transactions_%d",
			message.CurrentChunk, message.TotalChunks, receiverID)
		return nil
	case 1:
		queue := middleware.NewMessageMiddlewareQueue("transactions_items"+"_"+strconv.Itoa(receiverID), rh.Channel)
		payload := strings.Join(message.CSVRows, "\n")
		queue.Send([]byte(payload))
		log.Infof("Successfully forwarded batch (chunk %d/%d) to queue transactions_items_%d",
			message.CurrentChunk, message.TotalChunks, receiverID)
		return nil
	case 2:
		for i := 1; i <= rh.Config.ReceiversCount; i++ { // TODO: Configure menu items queue count properly
			queue := middleware.NewMessageMiddlewareQueue("menu_items"+"_"+strconv.Itoa(i), rh.Channel)
			payload := strings.Join(message.CSVRows, "\n")
			queue.Send([]byte(payload))
			log.Infof("Successfully forwarded batch (chunk %d/%d) to queue transactions_%d",
				message.CurrentChunk, message.TotalChunks, receiverID)

		}
		return nil
	}

	return nil
}

// sendEOFForFileType sends EOF message to all receiver queues for a specific fileType
func (rh *RequestHandler) sendEOFForFileType(fileType int) error {
	fileTypeInt := fileType

	var queuePrefix string
	switch fileTypeInt {
	case 0:
		queuePrefix = "transactions"
	case 1:
		queuePrefix = "transactions_items"
	case 2:
		queuePrefix = "menu_items"
	default:
		log.Warningf("Unknown fileType %d, skipping EOF send", fileType)
		return nil
	}

	for i := 1; i <= rh.Config.ReceiversCount; i++ {
		queueName := queuePrefix + "_" + strconv.Itoa(i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		queue.Send([]byte("EOF"))
		log.Infof("Successfully sent EOF to %s for fileType %d", queueName, fileType)
	}
	return nil
}

// waitForFinalResults waits for results from multiple queues
func (rh *RequestHandler) waitForFinalResults(resultChan chan ResultMessage, doneChan chan error, conn net.Conn) {
	resultsReceived := 0
	expectedResults := 4

	for resultsReceived < expectedResults {
		select {
		case result := <-resultChan:
			resultsReceived++
			log.Infof("Result %d/%d received from final_results_%d - Length: %d",
				resultsReceived, expectedResults, result.QueueID, len(result.Data))

			// Sort the result
			list := strings.Split(result.Data, "\n")
			slices.Sort(list)
			finalResult := strings.Join(list, "\n")

			// Send result back to client with queue ID
			if err := rh.sendResponse(conn, result.QueueID, finalResult); err != nil {
				log.Errorf("Failed to send response from queue %d: %v", result.QueueID, err)
				return
			}

			log.Infof("Successfully sent result %d/%d from final_results_%d",
				resultsReceived, expectedResults, result.QueueID)

		case err := <-doneChan:
			log.Errorf("Error while waiting for results: %v", err)
			return
		}
	}

	log.Infof("All %d results sent to client", resultsReceived)
}

// sendResponse writes the result back to the client in chunks with queue identifier
func (rh *RequestHandler) sendResponse(conn net.Conn, queueID int, result string) error {
	resultBytes := []byte(result)
	totalSize := len(resultBytes)

	if totalSize == 0 {
		log.Warningf("Empty result to send from queue %d", queueID)
		return rh.sendResultEOF(conn, queueID)
	}

	// Calculate number of chunks
	totalChunks := (totalSize + resultChunkSize - 1) / resultChunkSize
	log.Infof("Sending result from queue %d in %d chunks (total size: %d bytes)", queueID, totalChunks, totalSize)

	writer := bufio.NewWriter(conn)
	// reader := bufio.NewReader(conn)

	// Send each chunk
	for chunkNum := 1; chunkNum <= totalChunks; chunkNum++ {
		start := (chunkNum - 1) * resultChunkSize
		end := start + resultChunkSize
		if end > totalSize {
			end = totalSize
		}

		chunkData := resultBytes[start:end]
		chunkSize := len(chunkData)

		// Send chunk header with queue ID and size
		header := fmt.Sprintf("RESULT_CHUNK;%d;%d;%d;%d\n", queueID, chunkNum, totalChunks, chunkSize)
		if _, err := writer.WriteString(header); err != nil {
			return fmt.Errorf("failed to write chunk header: %w", err)
		}

		// Send chunk data (raw bytes, may contain newlines)
		if _, err := writer.Write(chunkData); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}

		// Send end-of-chunk marker (two newlines)
		if _, err := writer.WriteString("\n\n"); err != nil {
			return fmt.Errorf("failed to write chunk end marker: %w", err)
		}

		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush chunk: %w", err)
		}

		log.Infof("Sent result chunk %d/%d from queue %d (%d bytes)", chunkNum, totalChunks, queueID, chunkSize)

		// TODO: re-enable
		// Wait for ACK
		// if err := rh.waitForACK(reader); err != nil {
		// 	return fmt.Errorf("failed to receive ACK for chunk %d: %w", chunkNum, err)
		// }

		// log.Debugf("Received ACK for chunk %d/%d from queue %d", chunkNum, totalChunks, queueID)
	}

	// Send EOF after all chunks for this queue
	return rh.sendResultEOF(conn, queueID)
}

// sendResultEOF sends the result EOF marker to the client for a specific queue
func (rh *RequestHandler) sendResultEOF(conn net.Conn, queueID int) error {
	writer := bufio.NewWriter(conn)

	eofMsg := fmt.Sprintf("RESULT_EOF;%d\n", queueID)
	if _, err := writer.WriteString(eofMsg); err != nil {
		return fmt.Errorf("failed to write RESULT_EOF: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush RESULT_EOF: %w", err)
	}

	// TODO: re-enable
	// log.Infof("RESULT_EOF sent to client for queue %d, waiting for ACK...", queueID)

	// Wait for EOF ACK
	// reader := bufio.NewReader(conn)
	// if err := rh.waitForACK(reader); err != nil {
	// 	return fmt.Errorf("failed to receive RESULT_EOF ACK for queue %d: %w", queueID, err)
	// }

	// log.Infof("RESULT_EOF ACK received for queue %d", queueID)
	return nil
}

// waitForACK waits for an acknowledgment from the client
func (rh *RequestHandler) waitForACK(reader *bufio.Reader) error {
	ack, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("connection closed while waiting for ACK")
		}
		return fmt.Errorf("error reading ACK: %w", err)
	}

	ack = strings.TrimSpace(ack)
	if ack != "ACK" {
		return fmt.Errorf("expected ACK, got: %s", ack)
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

// createResultsCallback creates a callback function for consuming results from a specific queue
func createResultsCallback(resultChan chan ResultMessage, doneChan chan error, queueID int) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for response from final_results_%d...", queueID)

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Channel closed for final_results_%d", queueID)
					doneChan <- fmt.Errorf("channel closed unexpectedly for queue %d", queueID)
					return
				}

				log.Infof("Message received from final_results_%d - Length: %d", queueID, len(msg.Body))

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to ack message from queue %d: %v", queueID, err)
				}

				resultChan <- ResultMessage{
					QueueID: queueID,
					Data:    string(msg.Body),
				}
			case err := <-done:
				doneChan <- err
			}
		}
	}
}
