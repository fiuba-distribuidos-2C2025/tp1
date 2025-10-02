package common

import (
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/protocol"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const (
	resultTimeout   = 60 * time.Second
	resultChunkSize = 10 * 1024 * 1024 // 10MB chunks for results
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

	proto := protocol.NewProtocol(conn)
	filesProcessed := 0

	// Keep processing messages until FINAL_EOF is received
	for {
		fileProcessed, isFileTypeEOF, isFinalEOF, fileType, err := rh.processMessages(proto)

		if err != nil {
			if err == io.EOF {
				log.Infof("Client closed connection after %d files", filesProcessed)
				return
			}
			log.Errorf("Failed to process messages: %v", err)
			return
		}

		if fileProcessed {
			filesProcessed++
			log.Infof("Successfully processed file %d", filesProcessed)
		}

		// Handle EOF for specific fileType
		if isFileTypeEOF {
			log.Infof("Received EOF for fileType %s after %d files", fileType, filesProcessed)

			// Send EOF to the appropriate RabbitMQ queues for this fileType
			if err := rh.sendEOFForFileType(fileType); err != nil {
				log.Errorf("Failed to send EOF for fileType %s to queues: %v", fileType, err)
				return
			}

			// Send ACK for this fileType EOF
			if err := proto.SendACK(); err != nil {
				log.Errorf("Failed to send EOF ACK for fileType %s: %v", fileType, err)
				return
			}

			log.Infof("Successfully sent EOF for fileType %s to queues", fileType)
			continue
		}

		// Handle FINAL_EOF
		if isFinalEOF {
			log.Infof("Received FINAL_EOF from client after %d files", filesProcessed)

			// Send ACK for FINAL_EOF
			if err := proto.SendACK(); err != nil {
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

	rh.waitForFinalResults(resultChan, doneChan, proto)
}

// processMessages handles incoming messages until a file is complete or EOF is received
// Returns (fileProcessed, isFileTypeEOF, isFinalEOF, fileType, error)
func (rh *RequestHandler) processMessages(proto *protocol.Protocol) (bool, bool, bool, protocol.FileType, error) {
	var lastFileHash string
	var totalChunks int32
	chunksReceived := int32(0)

	log.Debug("Starting to process new messages")

	// Keep reading messages until all chunks are received for this file
	for {
		log.Debug("Reading next message...")
		msgType, data, err := proto.ReceiveMessage()
		if err != nil {
			log.Errorf("Error reading message: %v", err)
			return false, false, false, 0, err
		}

		switch msgType {
		case protocol.MessageTypeFinalEOF:
			log.Info("FINAL_EOF marker detected")
			return false, false, true, 0, nil

		case protocol.MessageTypeEOF:
			eofMsg := data.(*protocol.EOFMessage)
			log.Infof("FileType EOF marker detected for fileType %s", eofMsg.FileType)
			return false, true, false, eofMsg.FileType, nil

		case protocol.MessageTypeBatch:
			message := data.(*protocol.BatchMessage)

			// Track file transfer progress
			if lastFileHash == "" {
				lastFileHash = fmt.Sprintf("file_%s", message.FileType)
				totalChunks = message.TotalChunks
				log.Infof("Starting new file with %d total chunks (FileType: %s)",
					totalChunks, message.FileType)
			}

			chunksReceived++
			log.Infof("Received batch (chunk %d/%d) with %d rows (FileType: %s)",
				message.CurrentChunk, message.TotalChunks,
				len(message.CSVRows), message.FileType)

			// Process the message through RabbitMQ
			receiverID := rh.currentWorkerQueue
			if receiverID > rh.Config.ReceiversCount {
				rh.currentWorkerQueue = 1
				receiverID = rh.currentWorkerQueue
			}

			if err := rh.sendToQueue(message, receiverID); err != nil {
				return false, false, false, 0, fmt.Errorf("failed to send to queue: %w", err)
			}

			rh.currentWorkerQueue++

			// Send ACK after successfully processing the chunk
			if err := proto.SendACK(); err != nil {
				return false, false, false, 0, fmt.Errorf("failed to send ACK: %w", err)
			}
			log.Debugf("Sent ACK for chunk %d/%d", message.CurrentChunk, message.TotalChunks)

			// Check if all chunks have been received
			if chunksReceived >= totalChunks {
				log.Infof("All %d chunks received", chunksReceived)
				return true, false, false, 0, nil
			}

		default:
			return false, false, false, 0, fmt.Errorf("unexpected message type: 0x%02x", msgType)
		}
	}
}

// sendToQueue sends the batch message to the appropriate queue based on file type
func (rh *RequestHandler) sendToQueue(message *protocol.BatchMessage, receiverID int) error {
	payload := strings.Join(message.CSVRows, "\n")

	switch message.FileType {
	case protocol.FileTypeTransactions:
		queueName := "transactions_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeTransactionItems:
		queueName := "transactions_items_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeStores:
		// Broadcast stores data to all receivers
		for i := 1; i <= rh.Config.ReceiversCount; i++ {
			queueName := "stores_" + strconv.Itoa(i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
			queue.Send([]byte(payload))
		}
		log.Infof("Broadcasted batch (chunk %d/%d) to all stores queues",
			message.CurrentChunk, message.TotalChunks)

	case protocol.FileTypeMenuItems:
		// Broadcast menu items data to all receivers
		for i := 1; i <= rh.Config.ReceiversCount; i++ {
			queueName := "menu_items_" + strconv.Itoa(i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
			queue.Send([]byte(payload))
		}
		log.Infof("Broadcasted batch (chunk %d/%d) to all menu_items queues",
			message.CurrentChunk, message.TotalChunks)

	case protocol.FileTypeUsers:
		queueName := "users_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)
	default:
		return fmt.Errorf("unknown file type: %d", message.FileType)
	}

	return nil
}

// sendEOFForFileType sends EOF message to all receiver queues for a specific fileType
func (rh *RequestHandler) sendEOFForFileType(fileType protocol.FileType) error {
	queuePrefix := fileType.QueueName()
	if queuePrefix == "" {
		log.Warningf("Unknown fileType %d, skipping EOF send", fileType)
		return nil
	}

	for i := 1; i <= rh.Config.ReceiversCount; i++ {
		queueName := queuePrefix + "_" + strconv.Itoa(i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, rh.Channel)
		queue.Send([]byte("EOF"))
		log.Infof("Successfully sent EOF to %s for fileType %s", queueName, fileType)
	}
	return nil
}

// waitForFinalResults waits for results from multiple queues
func (rh *RequestHandler) waitForFinalResults(resultChan chan ResultMessage, doneChan chan error, proto *protocol.Protocol) {
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

			// Send result back to client
			if err := rh.sendResponse(proto, int32(result.QueueID), []byte(finalResult)); err != nil {
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

// sendResponse writes the result back to the client in chunks
func (rh *RequestHandler) sendResponse(proto *protocol.Protocol, queueID int32, result []byte) error {
	totalSize := len(result)

	if totalSize == 0 {
		log.Warningf("Empty result to send from queue %d", queueID)
		return proto.SendResultEOF(queueID)
	}

	// Calculate number of chunks
	totalChunks := int32((totalSize + resultChunkSize - 1) / resultChunkSize)
	log.Infof("Sending result from queue %d in %d chunks (total size: %d bytes)",
		queueID, totalChunks, totalSize)

	// Send each chunk
	for chunkNum := int32(1); chunkNum <= totalChunks; chunkNum++ {
		start := int((chunkNum - 1) * int32(resultChunkSize))
		end := start + resultChunkSize
		if end > totalSize {
			end = totalSize
		}

		chunkData := result[start:end]

		chunkMsg := &protocol.ResultChunkMessage{
			QueueID:      queueID,
			CurrentChunk: chunkNum,
			TotalChunks:  totalChunks,
			Data:         chunkData,
		}

		if err := proto.SendResultChunk(chunkMsg); err != nil {
			return fmt.Errorf("failed to send chunk %d: %w", chunkNum, err)
		}

		log.Infof("Sent result chunk %d/%d from queue %d (%d bytes)",
			chunkNum, totalChunks, queueID, len(chunkData))
	}

	// Send EOF after all chunks for this queue
	return proto.SendResultEOF(queueID)
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
				return // Only consume one message per queue

			case err := <-done:
				doneChan <- err
				return
			}
		}
	}
}
