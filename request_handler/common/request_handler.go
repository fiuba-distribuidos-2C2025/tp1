package common

import (
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
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
	expectedResultCount = 4 // Number of final result queues
)

// ResultMessage contains a result and which queue it came from
type ResultMessage struct {
	QueueID int
	Data    string
}

// RequestHandlerConfig holds configuration for the request handler
type RequestHandlerConfig struct {
	Port                           string
	IP                             string
	MiddlewareURL                  string
	TransactionsReceiversCount     int
	TransactionItemsReceiversCount int
	StoresQ3ReceiversCount         int
	StoresQ4ReceiversCount         int
	MenuItemsReceiversCount        int
	UsersReceiversCount            int
	BufferSize                     int
}

// RequestHandler handles incoming client connections and manages message flow
type RequestHandler struct {
	Config     RequestHandlerConfig
	listener   net.Listener
	shutdown   chan struct{}
	Connection *amqp.Connection
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
	log.Infof("Starting request handler with config %+v", rh.Config)

	addr := net.JoinHostPort(rh.Config.IP, rh.Config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	rh.listener = listener
	log.Infof("RequestHandler listening on %s", addr)

	// Connect to RabbitMQ
	conn, err := amqp.Dial(rh.Config.MiddlewareURL)
	if err != nil {
		rh.listener.Close()
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	rh.Connection = conn
	log.Infof("Connected to RabbitMQ at %s", rh.Config.MiddlewareURL)

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

	if rh.Connection != nil {
		if err := rh.Connection.Close(); err != nil {
			log.Errorf("Error closing AMQP connection: %v", err)
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
		channel, err := rh.Connection.Channel()
		if err != nil {
			log.Errorf("Failed to create channel for new connection: %v", err)
			conn.Close()
			continue
		}

		go handleNewConnection(conn, rh.Config, channel)
	}
}

// handleNewConnection processes a single client connection
func handleNewConnection(conn net.Conn, cfg RequestHandlerConfig, channel *amqp.Channel) {
	defer conn.Close()
	defer channel.Close()
	log.Infof("New connection from %s", conn.RemoteAddr())

	proto := protocol.NewProtocol(conn)
	msgType, data, err := proto.ReceiveMessage()
	if err != nil {
		log.Errorf("Failed to receive initial message from %s: %v", conn.RemoteAddr(), err)
		return
	}

	switch msgType {
	case protocol.MessageTypeQueryRequest:
		// TODO: handle errors here?
		handleQueryRequest(proto, cfg, channel)
	case protocol.MessageTypeResultsRequest:
		queryId := data.(*protocol.ResultRequestMessage).QueryID
		requestedResults := data.(*protocol.ResultRequestMessage).RequestedResults
		// TODO: handle errors here?
		handleResultsRequest(proto, cfg, channel, queryId, requestedResults, cfg.BufferSize)
	default:
		log.Errorf("Unknown message type %d from %s", msgType, conn.RemoteAddr())
		return
	}
}

func handleQueryRequest(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel) {
	filesProcessed := 0
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, 4)
	for i := range bytes {
		bytes[i] = byte(src.Intn(256))
	}
	clientId := hex.EncodeToString(bytes)

	// Keep processing messages until FINAL_EOF is received
	for {
		fileProcessed, isFileTypeEOF, isFinalEOF, fileType, err := processMessages(proto, cfg, channel, clientId)

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
			log.Infof("Received EOF from client %d after %d files", clientId, filesProcessed)

			// Send EOF to the appropriate RabbitMQ queues for this fileType
			if err := sendEOFForFileType(clientId, fileType, cfg, channel); err != nil {
				log.Errorf("Failed to send EOF for fileType %s: %v", fileType, err)
				return
			}

			// Send ACK for this fileType EOF
			if err := proto.SendACK(); err != nil {
				log.Errorf("Failed to send EOF ACK for fileType %s: %v", fileType, err)
				return
			}

			log.Infof("Successfully sent EOF to queues from client %d", clientId)
			continue
		}

		// Handle FINAL_EOF
		if isFinalEOF {
			log.Infof("Received FINAL_EOF from client %d after %d files", clientId, filesProcessed)

			// Send Query ID after FINAL_EOF
			if err := proto.SendQueryId(clientId); err != nil {
				log.Errorf("Failed to send Queue Id: %v", err)
				return
			}

			log.Infof("Successfully processed all files and received FINAL_EOF from client %d", clientId)
			break
		}
	}

	// Process final results with proper cleanup
	if err := processFinalResults(clientId, channel, proto); err != nil {
		log.Errorf("Failed to process final results: %v", err)
	}
}

// processFinalResults handles consuming and sending final results
func processFinalResults(clientId string, channel *amqp.Channel, proto *protocol.Protocol) error {
	resultChan := make(chan ResultMessage, expectedResultCount)
	errChan := make(chan error, expectedResultCount)

	// Start consuming from all final result queues
	for i := 1; i <= expectedResultCount; i++ {
		queueName := fmt.Sprintf("final_results_%s_%d", clientId, i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)

		go func(qID int, q *middleware.MessageMiddlewareQueue) {
			consumeOneResult(q, qID, resultChan, errChan)
		}(i, queue)

		log.Infof("Client %s: Started consuming from %s", clientId, queueName)
	}

	return waitForFinalResults(resultChan, errChan, proto, clientId)
}

// consumeOneResult consumes exactly one message from a queue
func consumeOneResult(queue *middleware.MessageMiddlewareQueue, queueID int, resultChan chan ResultMessage, errChan chan error) {
	queue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, doneCh chan error) {
		select {
		case msg, ok := <-*consumeChannel:
			if !ok {
				errChan <- fmt.Errorf("channel closed unexpectedly for queue %d", queueID)
				return
			}

			if err := msg.Ack(false); err != nil {
				log.Errorf("Failed to ack message from queue %d: %v", queueID, err)
			}

			resultChan <- ResultMessage{
				QueueID: queueID,
				Data:    string(msg.Body),
			}

		case err := <-doneCh:
			errChan <- err
		}
	})
}

// processMessages handles incoming messages until a file is complete or EOF is received
// Returns (clientId, fileProcessed, isFileTypeEOF, isFinalEOF, fileType, error)
func processMessages(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, clientId string) (bool, bool, bool, protocol.FileType, error) {
	var totalChunks int32
	chunksReceived := int32(0)

	log.Debug("Starting to process new messages")

	// Keep reading messages until all chunks are received for this file
	currentWorkerQueue := 0
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

			chunksReceived++
			log.Infof("Received batch: chunk %d/%d with %d rows",
				message.CurrentChunk, message.TotalChunks, len(message.CSVRows))

			// Initialize tracking variables on first chunk
			if chunksReceived == 1 {
				totalChunks = message.TotalChunks
			}

			// Process each message through RabbitMQ
			receiverID := currentWorkerQueue
			switch message.FileType {
			case protocol.FileTypeTransactions:
				receiverID = receiverID%cfg.TransactionsReceiversCount + 1
			case protocol.FileTypeTransactionItems:
				receiverID = receiverID%cfg.TransactionItemsReceiversCount + 1
			case protocol.FileTypeMenuItems:
				receiverID = receiverID%cfg.MenuItemsReceiversCount + 1
			case protocol.FileTypeUsers:
				receiverID = receiverID%cfg.UsersReceiversCount + 1
			}
			log.Infof("Calculated receiver %d", receiverID)

			if err := sendToQueue(message, receiverID, cfg, channel, clientId); err != nil {
				return false, false, false, 0, fmt.Errorf("failed to send to queue: %w", err)
			}

			currentWorkerQueue++

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
		}
	}
}

// sendToQueue sends the batch message to the appropriate queue based on file type
func sendToQueue(message *protocol.BatchMessage, receiverID int, cfg RequestHandlerConfig, channel *amqp.Channel, clientId string) error {
	internalUUID, err := protocol.ClientIDFromString(clientId)
	if err != nil {
		return fmt.Errorf("failed to parse client ID: %w", err)
	}

	var payload strings.Builder
	clientIdStr := protocol.ClientIDToString(internalUUID)
	msgID := fmt.Sprintf("%d", rand.Int63())
	payload.WriteString(clientIdStr)
	payload.WriteString("\n")
	payload.WriteString(msgID)
	payload.WriteString("\n")
	payload.WriteString(strings.Join(message.CSVRows, "\n"))
	payloadBytes := []byte(payload.String())

	switch message.FileType {
	case protocol.FileTypeTransactions:
		queueName := "transactions_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send(payloadBytes)
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeTransactionItems:
		queueName := "transactions_items_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send(payloadBytes)
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeStores:
		// Broadcast stores data to all receivers
		for i := 1; i <= cfg.StoresQ3ReceiversCount; i++ {
			queue_q3 := middleware.NewMessageMiddlewareQueue("stores_q3_"+strconv.Itoa(i), channel)
			queue_q3.Send(payloadBytes)
			log.Infof("Successfully forwarded batch (chunk %d/%d) to queue stores_q3_%d",
				message.CurrentChunk, message.TotalChunks, i)
		}

		for i := 1; i <= cfg.StoresQ4ReceiversCount; i++ {
			queue_q4 := middleware.NewMessageMiddlewareQueue("stores_q4_"+strconv.Itoa(i), channel)
			queue_q4.Send(payloadBytes)
			log.Infof("Successfully forwarded batch (chunk %d/%d) to queue stores_q4_%d",
				message.CurrentChunk, message.TotalChunks, i)
		}
		log.Infof("Broadcasted batch (chunk %d/%d) to all stores queues",
			message.CurrentChunk, message.TotalChunks)

	case protocol.FileTypeMenuItems:
		// Broadcast menu items data to all receivers
		for i := 1; i <= cfg.MenuItemsReceiversCount; i++ {
			queueName := "menu_items_" + strconv.Itoa(i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send(payloadBytes)
		}
		log.Infof("Broadcasted batch (chunk %d/%d) to all menu_items queues",
			message.CurrentChunk, message.TotalChunks)

	case protocol.FileTypeUsers:
		queueName := "users_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send(payloadBytes)
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)
	default:
		return fmt.Errorf("unknown file type: %d", message.FileType)
	}

	return nil
}

// sendEOFForFileType sends EOF message to all receiver queues for a specific fileType
func sendEOFForFileType(clientId string, fileType protocol.FileType, cfg RequestHandlerConfig, channel *amqp.Channel) error {
	log.Infof("Sending EOF from client id %s", clientId)

	var payload strings.Builder
	payload.WriteString(clientId)

	// generate a random message ID
	// this is a workaround until we refactor the request handler to
	// properly handle message IDs
	msgID := fmt.Sprintf("%d", rand.Int63())
	payload.WriteString(msgID)

	payload.WriteString("\nEOF")
	payloadBytes := []byte(payload.String())

	queuePrefix := fileType.QueueName()
	if queuePrefix == "" {
		log.Warningf("Unknown fileType %d, skipping EOF send", fileType)
		return nil
	}

	var receiversCount int
	switch queuePrefix {
	case "transactions":
		receiversCount = cfg.TransactionsReceiversCount
	case "transactions_items":
		receiversCount = cfg.TransactionItemsReceiversCount
	case "users":
		receiversCount = cfg.UsersReceiversCount
	case "menu_items":
		receiversCount = cfg.MenuItemsReceiversCount
	// Special case for stores since we use them in two different queries
	case "stores":
		for i := 1; i <= cfg.StoresQ3ReceiversCount; i++ {
			queueName := "stores_q3_" + strconv.Itoa(i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send(payloadBytes)
			log.Infof("Successfully sent EOF to %s for client %s", queueName, clientId)
		}

		for i := 1; i <= cfg.StoresQ4ReceiversCount; i++ {
			queueName := "stores_q4_" + strconv.Itoa(i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send(payloadBytes)
			log.Infof("Successfully sent EOF to %s for client %s", queueName, clientId)
		}
		return nil
	}

	for i := 1; i <= receiversCount; i++ {
		queueName := queuePrefix + "_" + strconv.Itoa(i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send(payloadBytes)
		log.Infof("Successfully sent EOF to %s for client %s", queueName, clientId)
	}
	return nil
}

// waitForFinalResults waits for results from multiple queues
func waitForFinalResults(resultChan chan ResultMessage, errChan chan error, proto *protocol.Protocol, clientId string) error {
	resultsReceived := 0

	for resultsReceived < expectedResultCount {
		select {
		case result, ok := <-resultChan:
			if !ok {
				return fmt.Errorf("result channel closed unexpectedly")
			}
			resultsReceived++

			list := strings.Split(result.Data, "\n")
			slices.Sort(list)
			finalResult := strings.Join(list, "\n")

			// Persist result to disk
			if err := saveResponse(clientId, int32(result.QueueID), []byte(finalResult)); err != nil {
				return fmt.Errorf("failed to save response from queue %d: %w", result.QueueID, err)
			}

			log.Infof("Successfully saved result %d/%d (final_results_%d)",
				resultsReceived, expectedResultCount, result.QueueID)

		case err := <-errChan:
			return fmt.Errorf("error while waiting for results: %w", err)
		}
	}

	log.Infof("All %d results persisted", resultsReceived)
	return nil
}

// saveResponse saves the query result in a file
func saveResponse(queryId string, queueID int32, data []byte) error {
	log.Debugf("Saving results of queue %d for client %s", queueID, queryId)
	fileName := fmt.Sprintf("final_results_%d_%s.txt", queueID, queryId)
	return os.WriteFile(fileName, data, 0644)
}

func handleResultsRequest(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, queryId string, requestedResults []int, bufferSize int) error {
	log.Infof("Handling results request for query %s", queryId)

	// Check if at least one of the requested files is present
	availableResults := checkAvailableResults(queryId, requestedResults)
	if len(availableResults) == 0 {
		log.Debugf("None of the results for query %s are present", queryId)
		return proto.SendResultsPending()
	}

	err := proto.SendResultsReady()
	if err != nil {
		return err
	}

	for _, queueID := range availableResults {
		fileName := fmt.Sprintf("final_results_%d_%s.txt", queueID, queryId)
		result, err := os.ReadFile(fileName)
		if err != nil {
			return fmt.Errorf("error reading file %s: %w", fileName, err)
		}
		err = sendResponse(proto, int32(queueID), result, bufferSize)
		if err != nil {
			return err
		}
	}

	return nil
}

// Check if at least one of the requested files is available
func checkAvailableResults(queryId string, requestedResults []int) []int {
	var availableResults []int
	for _, queueID := range requestedResults {
		fileName := fmt.Sprintf("final_results_%d_%s.txt", queueID, queryId)
		_, err := os.Stat(fileName)
		if err == nil {
			availableResults = append(availableResults, queueID)
		}
	}
	return availableResults
}

func sendResponse(proto *protocol.Protocol, queueID int32, result []byte, bufferSize int) error {
	totalSize := len(result)

	if totalSize == 0 {
		log.Warningf("Empty result to send from queue %d", queueID)
		return proto.SendResultEOF(queueID)
	}

	// Calculate number of chunks
	totalChunks := int32((totalSize + bufferSize - 1) / bufferSize)

	// Send each chunk
	for chunkNum := int32(1); chunkNum <= totalChunks; chunkNum++ {
		start := int((chunkNum - 1) * int32(bufferSize))
		end := start + bufferSize
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
	}

	// Send EOF after all chunks for this queue
	return proto.SendResultEOF(queueID)
}
