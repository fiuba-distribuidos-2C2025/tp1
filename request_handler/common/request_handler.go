package common

import (
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/healthcheck"
	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/protocol"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const (
	OwnResultsDir      = "results_own"      // Results directory with results for clients that this handler served
	ExternalResultsDir = "results_external" // Results directory with results for clients served by other handlers
)

// ResultMessage contains a result and which queue it came from
type ResultMessage struct {
	ClientID string
	QueueID  int
	Data     string
}

// RequestHandlerConfig holds configuration for the request handler
type RequestHandlerConfig struct {
	Port                           string
	IP                             string
	ID                             int
	MiddlewareURL                  string
	TransactionsReceiversCount     int
	TransactionItemsReceiversCount int
	StoresQ3ReceiversCount         int
	StoresQ4ReceiversCount         int
	MenuItemsReceiversCount        int
	UsersReceiversCount            int
	BufferSize                     int
	MaxClientResultsMinutes        int
}

// RequestHandler handles incoming client connections and manages message flow
type RequestHandler struct {
	Config       RequestHandlerConfig
	listener     net.Listener
	shutdown     chan struct{}
	Connection   *amqp.Connection
	queueConfigs []struct {
		prefix string
		count  int
	}
}

// ResultListener manages listening to a specific result queue
type ResultListener struct {
	queueID   int
	handlerID int
	channel   *amqp.Channel
	queue     *middleware.MessageMiddlewareQueue
	shutdown  chan struct{}
	mu        sync.Mutex
}

// NewRequestHandler creates a new RequestHandler instance
func NewRequestHandler(config RequestHandlerConfig) *RequestHandler {
	healthcheck.InitHealthChecker()

	// Create result directories
	if err := os.MkdirAll(OwnResultsDir, 0755); err != nil {
		log.Errorf("Failed to create own results directory: %v", err)
	}
	if err := os.MkdirAll(ExternalResultsDir, 0755); err != nil {
		log.Errorf("Failed to create external results directory: %v", err)
	}

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
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	rh.Connection = conn
	log.Infof("Connected to RabbitMQ at %s", rh.Config.MiddlewareURL)

	go rh.startResultListeners()
	defer rh.listener.Close()

	go rh.acceptConnections()
	go rh.monitorClientFiles()

	<-rh.shutdown
	return nil
}

// Monitors result files and performs cleanup based on timestamp
func (rh *RequestHandler) monitorClientFiles() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ownPattern := filepath.Join(OwnResultsDir, "final_results_*_*.txt")
			ownFiles, err := filepath.Glob(ownPattern)
			if err != nil {
				log.Errorf("Error finding own result files: %v", err)
			}

			externalPattern := filepath.Join(ExternalResultsDir, "final_results_*_*.txt")
			externalFiles, err := filepath.Glob(externalPattern)
			if err != nil {
				log.Errorf("Error finding external result files: %v", err)
			}

			ownClientFiles := make(map[string][]string)
			ownClientTimestamps := make(map[string]time.Time)

			for _, filePath := range ownFiles {
				timestamp, clientID, _, err := parseResultFileName(filePath)
				if err != nil {
					log.Debugf("Skipping file %s: %v", filePath, err)
					continue
				}

				ownClientFiles[clientID] = append(ownClientFiles[clientID], filePath)
				if existingTime, exists := ownClientTimestamps[clientID]; !exists || timestamp.Before(existingTime) {
					ownClientTimestamps[clientID] = timestamp
				}
			}

			for clientID, files := range ownClientFiles {
				timestamp := ownClientTimestamps[clientID]
				age := time.Since(timestamp)

				if age > time.Duration(rh.Config.MaxClientResultsMinutes)*time.Minute {
					log.Infof("Client %s has expired (age: %v), sending cleanup messages", clientID, age)

					if err := rh.sendClientCleanupMessages(clientID); err != nil {
						log.Errorf("Failed to send cleanup messages for client %s: %v", clientID, err)
						continue
					}

					for _, filePath := range files {
						if err := os.Remove(filePath); err != nil {
							log.Errorf("Failed to remove file %s: %v", filePath, err)
						} else {
							log.Infof("Removed expired result file: %s", filePath)
						}
					}
				}
			}

			// Remove the external files because another request handler is responsible for cleanup
			for _, filePath := range externalFiles {
				timestamp, clientID, queueID, err := parseResultFileName(filePath)
				if err != nil {
					log.Debugf("Skipping file %s: %v", filePath, err)
					continue
				}

				age := time.Since(timestamp)
				if age > time.Duration(rh.Config.MaxClientResultsMinutes)*time.Minute {
					log.Infof("Removing external result file for client %s, queue %d (age: %v)",
						clientID, queueID, age)

					if err := os.Remove(filePath); err != nil {
						log.Errorf("Failed to remove external file %s: %v", filePath, err)
					} else {
						log.Infof("Removed external result file: %s", filePath)
					}
				}
			}

		case <-rh.shutdown:
			return
		}
	}
}

// This tells all workers to remove any data associated with this client
func (rh *RequestHandler) sendClientCleanupMessages(clientID string) error {
	log.Infof("Sending cleanup messages for client %s to all queues", clientID)
	channel, err := rh.Connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	var payload strings.Builder
	payload.WriteString(clientID)
	payload.WriteString("\n0")
	payload.WriteString("\nCLEANUP")
	payloadBytes := []byte(payload.String())

	queueConfigs := []struct {
		prefix string
		count  int
	}{
		{"transactions", rh.Config.TransactionsReceiversCount},
		{"transactions_items", rh.Config.TransactionItemsReceiversCount},
		{"users", rh.Config.UsersReceiversCount},
		{"menu_items", rh.Config.MenuItemsReceiversCount},
		{"stores_q3", rh.Config.StoresQ3ReceiversCount},
		{"stores_q4", rh.Config.StoresQ4ReceiversCount},
	}

	for _, config := range queueConfigs {
		for i := 1; i <= config.count; i++ {
			queueName := fmt.Sprintf("%s_%d", config.prefix, i)
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send(payloadBytes)
			log.Debugf("Sent cleanup message to queue %s for client %s", queueName, clientID)
		}
	}

	log.Infof("Successfully sent cleanup messages for client %s to all queues", clientID)
	return nil
}

func (rh *RequestHandler) startResultListeners() error {
	log.Infof("Starting %d result queue listeners", 4)

	for i := 1; i <= 4; i++ {
		channel, err := rh.Connection.Channel()
		if err != nil {
			return fmt.Errorf("failed to create channel for result listener %d: %w", i, err)
		}

		queueName := fmt.Sprintf("final_results_%d_%d", rh.Config.ID, i)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)

		listener := &ResultListener{
			queueID:   i,
			handlerID: rh.Config.ID,
			channel:   channel,
			queue:     queue,
			shutdown:  make(chan struct{}),
		}

		listener.startListening()
		log.Infof("Started background listener for %s", queueName)
	}

	return nil
}

// startListening continuously listens for results on this queue
func (rl *ResultListener) startListening() {
	queueName := fmt.Sprintf("final_results_%d_%d", rl.handlerID, rl.queueID)
	log.Infof("Result listener %d started consuming from %s", rl.queueID, queueName)

	rl.queue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, doneCh chan error) {
		for {
			select {
			case <-rl.shutdown:
				log.Infof("Result listener %d shutting down", rl.queueID)
				return

			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Errorf("Channel closed for result queue %d", rl.queueID)
					return
				}

				parsedMsg := strings.SplitN(string(msg.Body), "\n", 2)
				clientID := parsedMsg[0]
				data := parsedMsg[1]

				if err := rl.processResult(clientID, data); err != nil {
					log.Errorf("Failed to process result from queue %d for client %s: %v",
						rl.queueID, clientID, err)
					continue
				}

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to ack message from queue %d: %v", rl.queueID, err)
				}

				log.Infof("Successfully processed result from queue %d for client %s",
					rl.queueID, clientID)

			case err := <-doneCh:
				if err != nil {
					log.Errorf("Error in result listener %d: %v", rl.queueID, err)
				}
				return
			}
		}
	})
}

// processResult saves the result to disk
func (rl *ResultListener) processResult(clientID string, data string) error {
	list := strings.Split(data, "\n")
	slices.Sort(list)
	finalResult := strings.Join(list, "\n")

	return saveResponse(clientID, int32(rl.queueID), []byte(finalResult))
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

		go rh.handleNewConnection(conn, channel)
	}
}

// handleNewConnection processes a single client connection
func (rh *RequestHandler) handleNewConnection(conn net.Conn, channel *amqp.Channel) {
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
	case protocol.MessageTypeHealthCheck:
		log.Debugf("Health check from proxy")
		proto.SendHealthCheckResponse()
		return
	case protocol.MessageTypeQueryRequest:
		rh.handleQueryRequest(proto, channel)
	case protocol.MessageTypeResultsRequest:
		queryId := data.(*protocol.ResultRequestMessage).QueryID
		requestedResults := data.(*protocol.ResultRequestMessage).RequestedResults
		handleResultsRequest(proto, rh.Config, channel, queryId, requestedResults, rh.Config.BufferSize)
	case protocol.MessageTypeResumeRequest:
		queryId := data.(*protocol.ResumeRequestMessage).QueryID
		handleResumeRequest(proto, rh.Config, channel, queryId)
	default:
		log.Errorf("Unknown message type %d from %s", msgType, conn.RemoteAddr())
		return
	}
}

func (rh *RequestHandler) handleQueryRequest(proto *protocol.Protocol, channel *amqp.Channel) {
	nowInstant := time.Now()
	src := rand.New(rand.NewSource(nowInstant.UnixNano()))
	bytes := make([]byte, 4)
	for i := range bytes {
		bytes[i] = byte(src.Intn(256))
	}
	clientId := hex.EncodeToString(bytes)

	if err := createClientResultFiles(clientId, nowInstant); err != nil {
		log.Errorf("Failed to create result files for client %s: %v", clientId, err)
		return
	}

	if err := proto.SendQueryId(clientId); err != nil {
		log.Errorf("Failed to send Queue Id: %v", err)
		return
	}

	processClientMessages(proto, rh.Config, channel, clientId)
}

func handleResumeRequest(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, clientId string) {
	log.Infof("Resuming query processing for client %s", clientId)

	// Send ACK to confirm we're ready to resume
	if err := proto.SendACK(); err != nil {
		log.Errorf("Failed to send resume ACK: %v", err)
		return
	}

	processClientMessages(proto, cfg, channel, clientId)
}

// processClientMessages handles the common message processing loop for both new and resumed queries
func processClientMessages(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, clientId string) {
	filesProcessed := 0

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
			log.Infof("Received EOF from client %s after %d files", clientId, filesProcessed)

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

			log.Infof("Successfully sent EOF to queues from client %s", clientId)
			continue
		}

		// Handle FINAL_EOF
		if isFinalEOF {
			log.Infof("Successfully processed all files and received FINAL_EOF from client %s", clientId)
			break
		}
	}

	// Results are now being processed in the background by result listeners
	log.Infof("Query %s submitted successfully. Results will be available when processing completes.", clientId)
}

// processMessages handles incoming messages until a file is complete or EOF is received
// Returns (fileProcessed, isFileTypeEOF, isFinalEOF, fileType, error)
func processMessages(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, clientId string) (bool, bool, bool, protocol.FileType, error) {
	var totalChunks int32

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

			log.Infof("Received batch: chunk %d/%d with %d rows",
				message.CurrentChunk, message.TotalChunks, len(message.CSVRows))

			// Initialize tracking variables on first chunk
			if message.CurrentChunk == 1 {
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
			if message.CurrentChunk >= totalChunks {
				log.Infof("All %d chunks received", totalChunks)
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
	payload.WriteString(clientIdStr)
	payload.WriteString("\n")
	msgID := fmt.Sprintf("%d", message.MsgID)
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
	msgID := fmt.Sprintf("\n%d", rand.Int63())
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

// Saves to OwnResultsDir if file exists there, otherwise creates in ExternalResultsDir
func saveResponse(queryId string, queueID int32, data []byte) error {
	log.Debugf("Saving results of queue %d for client %s", queueID, queryId)

	// First check in own results directory
	ownPattern := filepath.Join(OwnResultsDir, fmt.Sprintf("final_results_%d_*_%s.txt", queueID, queryId))
	ownMatches, err := filepath.Glob(ownPattern)
	if err != nil {
		return fmt.Errorf("failed to search for result file: %w", err)
	}

	var fileName string
	if len(ownMatches) > 0 {
		fileName = ownMatches[0]
		log.Infof("Writing results to own results file: %s", fileName)
	} else {
		externalPattern := filepath.Join(ExternalResultsDir, fmt.Sprintf("final_results_%d_*_%s.txt", queueID, queryId))
		externalMatches, err := filepath.Glob(externalPattern)
		if err != nil {
			return fmt.Errorf("failed to search for external result file: %w", err)
		}

		if len(externalMatches) > 0 {
			fileName = externalMatches[0]
			log.Infof("Writing results to existing external file: %s", fileName)
		} else {
			timestamp := time.Now().Unix()
			fileName = filepath.Join(ExternalResultsDir, fmt.Sprintf("final_results_%d_%d_%s.txt", queueID, timestamp, queryId))
			log.Infof("Creating new external result file: %s", fileName)
		}
	}

	return os.WriteFile(fileName, data, 0644)
}

func handleResultsRequest(proto *protocol.Protocol, cfg RequestHandlerConfig, channel *amqp.Channel, queryId string, requestedResults []int, bufferSize int) error {
	log.Infof("Handling results request for query %s", queryId)

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
		fileName := findResultFile(queueID, queryId)
		if fileName == "" {
			log.Errorf("Could not find result file for queue %d client %s", queueID, queryId)
			continue
		}

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

// Check if at least one of the requested files is available and has content
func checkAvailableResults(queryId string, requestedResults []int) []int {
	var availableResults []int
	for _, queueID := range requestedResults {
		fileName := findResultFile(queueID, queryId)
		if fileName == "" {
			continue
		}

		fileInfo, err := os.Stat(fileName)
		if err == nil && fileInfo.Size() > 0 {
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
