package common

import (
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	channelPoolSize = 10               // Number of channels in the pool
)

// ResultMessage contains a result and which queue it came from
type ResultMessage struct {
	QueueID int
	Data    string
}

// ChannelPool manages a pool of AMQP channels for concurrent use
type ChannelPool struct {
	channels chan *amqp.Channel
	conn     *amqp.Connection
	mu       sync.RWMutex
	closed   bool
}

// NewChannelPool creates a new channel pool
func NewChannelPool(conn *amqp.Connection, size int) (*ChannelPool, error) {
	pool := &ChannelPool{
		channels: make(chan *amqp.Channel, size),
		conn:     conn,
	}

	// Pre-populate the pool with channels
	for i := 0; i < size; i++ {
		ch, err := conn.Channel()
		if err != nil {
			// Close any channels we've already created
			close(pool.channels)
			for existingCh := range pool.channels {
				existingCh.Close()
			}
			return nil, fmt.Errorf("failed to create channel %d: %w", i, err)
		}
		pool.channels <- ch
	}

	return pool, nil
}

// Get retrieves a channel from the pool
func (p *ChannelPool) Get() (*amqp.Channel, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("channel pool is closed")
	}

	select {
	case ch := <-p.channels:
		return ch, nil
	default:
		// Pool is empty, create a new channel
		return p.conn.Channel()
	}
}

// Put returns a channel to the pool
func (p *ChannelPool) Put(ch *amqp.Channel) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed || ch == nil {
		if ch != nil {
			ch.Close()
		}
		return
	}

	select {
	case p.channels <- ch:
		// Channel returned to pool
	default:
		// Pool is full, close the channel
		ch.Close()
	}
}

// Close closes all channels in the pool
func (p *ChannelPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true
	close(p.channels)

	for ch := range p.channels {
		ch.Close()
	}
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
}

// RequestHandler handles incoming client connections and manages message flow
type RequestHandler struct {
	Config             RequestHandlerConfig
	listener           net.Listener
	shutdown           chan struct{}
	Connection         *amqp.Connection
	channelPool        *ChannelPool
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
	log.Infof("Starting request handler with config %+v", rh.Config)

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

	channelPool, err := NewChannelPool(rabbit_conn, channelPoolSize)
	if err != nil {
		rabbit_conn.Close()
		return fmt.Errorf("failed to create channel pool: %w", err)
	}

	rh.channelPool = channelPool
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
	if rh.channelPool != nil {
		rh.channelPool.Close()
	}

	log.Info("RequestHandler shutdown complete")
}

// acceptConnections continuously accepts new connections
// TODO: no graceful shutdown
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

		go handleConnection(conn, rh.Config, rh.channelPool)
	}
}

// handleConnection processes a single client connection
func handleConnection(conn net.Conn, cfg RequestHandlerConfig, channelPool *ChannelPool) {
	defer conn.Close()
	log.Infof("New connection from %s", conn.RemoteAddr())

	// Get a channel from the pool for this connection
	channel, err := channelPool.Get()
	if err != nil {
		log.Errorf("Failed to get channel for connection %s: %v", conn.RemoteAddr(), err)
		return
	}
	defer channelPool.Put(channel)

	proto := protocol.NewProtocol(conn)
	filesProcessed := 0

	// TODO: instead of "guessing" which client we are communicating with
	// It'd be more suitable to get this information on first message
	var clientId uint16

	// Keep processing messages until FINAL_EOF is received
	for {
		pclientId, fileProcessed, isFileTypeEOF, isFinalEOF, fileType, err := processMessages(proto, cfg, channelPool)

		// Not optimal check, only batch type messages hold clientId,
		// others return zero, so we ignore them
		if pclientId != 0 {
			clientId = pclientId
		}

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
			if err := sendEOFForFileType(clientId, fileType, cfg, channelPool); err != nil {
				log.Errorf("Failed to send EOF to queues: %v", fileType, err)
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

			// Send ACK for FINAL_EOF
			if err := proto.SendACK(); err != nil {
				log.Errorf("Failed to send FINAL_EOF ACK: %v", err)
				return
			}

			log.Infof("Successfully processed all files and received FINAL_EOF from client %d", clientId)
			break
		}
	}

	// Listen to multiple final result queues
	resultChan := make(chan ResultMessage, 4)
	doneChan := make(chan error, 1)

	// Start consuming from all final result queues
	for i := 1; i <= 4; i++ {
		queueName := fmt.Sprintf("final_results_%d_%d", clientId, i)
		// Get a channel from the pool for each consumer goroutine
		consumerChannel, err := channelPool.Get()
		if err != nil {
			log.Errorf("Failed to get consumer channel for %s: %v", queueName, err)
			continue
		}
		queue := middleware.NewMessageMiddlewareQueue(queueName, consumerChannel)
		go queue.StartConsuming(createResultsCallback(resultChan, doneChan, i, channelPool, consumerChannel))
		log.Infof("Client %d: Started consuming from %s", clientId, queueName)
	}

	waitForFinalResults(resultChan, doneChan, proto)
}

// processMessages handles incoming messages until a file is complete or EOF is received
// Returns (clientId, fileProcessed, isFileTypeEOF, isFinalEOF, fileType, error)
func processMessages(proto *protocol.Protocol, cfg RequestHandlerConfig, channelPool *ChannelPool) (uint16, bool, bool, bool, protocol.FileType, error) {
	var totalChunks int32
	chunksReceived := int32(0)
	var clientId uint16

	log.Debug("Starting to process new messages")

	// Keep reading messages until all chunks are received for this file
	currentWorkerQueue := 0
	for {
		log.Debug("Reading next message...")
		msgType, data, err := proto.ReceiveMessage()
		if err != nil {
			log.Errorf("Error reading message: %v", err)
			return clientId, false, false, false, 0, err
		}

		switch msgType {
		case protocol.MessageTypeFinalEOF:
			log.Info("FINAL_EOF marker detected")
			return clientId, false, false, true, 0, nil

		case protocol.MessageTypeEOF:
			eofMsg := data.(*protocol.EOFMessage)
			log.Infof("FileType EOF marker detected for fileType %s", eofMsg.FileType)
			return clientId, false, true, false, eofMsg.FileType, nil

		case protocol.MessageTypeBatch:
			message := data.(*protocol.BatchMessage)
			clientId = message.ClientID

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

			if err := sendToQueue(message, receiverID, cfg, channelPool); err != nil {
				return clientId, false, false, false, 0, fmt.Errorf("failed to send to queue: %w", err)
			}

			currentWorkerQueue++

			// Send ACK after successfully processing the chunk
			if err := proto.SendACK(); err != nil {
				return clientId, false, false, false, 0, fmt.Errorf("failed to send ACK: %w", err)
			}
			log.Debugf("Sent ACK for chunk %d/%d", message.CurrentChunk, message.TotalChunks)

			// Check if all chunks have been received
			if chunksReceived >= totalChunks {
				log.Infof("All %d chunks received", chunksReceived)
				return clientId, true, false, false, 0, nil
			}
		}
	}
}

// sendToQueue sends the batch message to the appropriate queue based on file type
func sendToQueue(message *protocol.BatchMessage, receiverID int, cfg RequestHandlerConfig, channelPool *ChannelPool) error {
	// Get a channel from the pool
	channel, err := channelPool.Get()
	if err != nil {
		return fmt.Errorf("failed to get channel from pool: %w", err)
	}
	defer channelPool.Put(channel)
	// First row of payload specifies the client ID
	payload := strconv.FormatUint(uint64(message.ClientID), 10) + "\n"
	// Rest of the payload is data itself
	payload += strings.Join(message.CSVRows, "\n")

	switch message.FileType {
	case protocol.FileTypeTransactions:
		queueName := "transactions_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeTransactionItems:
		queueName := "transactions_items_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)

	case protocol.FileTypeStores:
		// Broadcast stores data to all receivers
		for i := 1; i <= cfg.StoresQ3ReceiversCount; i++ {
			queue_q3 := middleware.NewMessageMiddlewareQueue("stores_q3_"+strconv.Itoa(i), channel)
			queue_q3.Send([]byte(payload))
			log.Infof("Successfully forwarded batch (chunk %d/%d) to queue stores_q3_%d",
				message.CurrentChunk, message.TotalChunks, i)
		}

		for i := 1; i <= cfg.StoresQ4ReceiversCount; i++ {
			queue_q4 := middleware.NewMessageMiddlewareQueue("stores_q4_"+strconv.Itoa(i), channel)
			queue_q4.Send([]byte(payload))
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
			queue.Send([]byte(payload))
		}
		log.Infof("Broadcasted batch (chunk %d/%d) to all menu_items queues",
			message.CurrentChunk, message.TotalChunks)

	case protocol.FileTypeUsers:
		queueName := "users_" + strconv.Itoa(receiverID)
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send([]byte(payload))
		log.Infof("Forwarded batch (chunk %d/%d) to queue %s",
			message.CurrentChunk, message.TotalChunks, queueName)
	default:
		return fmt.Errorf("unknown file type: %d", message.FileType)
	}

	return nil
}

// sendEOFForFileType sends EOF message to all receiver queues for a specific fileType
func sendEOFForFileType(clientId uint16, fileType protocol.FileType, cfg RequestHandlerConfig, channelPool *ChannelPool) error {
	log.Infof("Sending EOF from client id %d", clientId)
	// First row of payload specifies the client ID
	payload := strconv.FormatUint(uint64(clientId), 10) + "\n"
	payload += "EOF"

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
	// Special case for stores
	case "stores":
		for i := 1; i <= cfg.StoresQ3ReceiversCount; i++ {
			queueName := "stores_q3_" + strconv.Itoa(i)
			channel, err := channelPool.Get()
			if err != nil {
				return fmt.Errorf("failed to get channel for %s: %w", queueName, err)
			}
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send([]byte(payload))
			channelPool.Put(channel)
			log.Infof("Successfully sent EOF to %s for client %d", queueName, clientId)
		}

		for i := 1; i <= cfg.StoresQ4ReceiversCount; i++ {
			queueName := "stores_q4_" + strconv.Itoa(i)
			channel, err := channelPool.Get()
			if err != nil {
				return fmt.Errorf("failed to get channel for %s: %w", queueName, err)
			}
			queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
			queue.Send([]byte(payload))
			channelPool.Put(channel)
			log.Infof("Successfully sent EOF to %s for client %d", queueName, clientId)
		}
		return nil
	}

	for i := 1; i <= receiversCount; i++ {
		queueName := queuePrefix + "_" + strconv.Itoa(i)
		channel, err := channelPool.Get()
		if err != nil {
			return fmt.Errorf("failed to get channel for %s: %w", queueName, err)
		}
		queue := middleware.NewMessageMiddlewareQueue(queueName, channel)
		queue.Send([]byte(payload))
		channelPool.Put(channel)
		log.Infof("Successfully sent EOF to %s for client %d", queueName, clientId)
	}
	return nil
}

// waitForFinalResults waits for results from multiple queues
func waitForFinalResults(resultChan chan ResultMessage, doneChan chan error, proto *protocol.Protocol) {
	resultsReceived := 0
	expectedResults := 4

	for resultsReceived < expectedResults {
		select {
		case result := <-resultChan:
			resultsReceived++

			// Sort the result
			list := strings.Split(result.Data, "\n")
			slices.Sort(list)
			finalResult := strings.Join(list, "\n")

			// Send result back to client
			if err := sendResponse(proto, int32(result.QueueID), []byte(finalResult)); err != nil {
				log.Errorf("Failed to send response from queue %d: %v", result.QueueID, err)
				return
			}

			log.Infof("Successfully sent result %d/%d (final_results_%d)",
				resultsReceived, expectedResults, result.QueueID)

		case err := <-doneChan:
			log.Errorf("Error while waiting for results: %v", err)
			return
		}
	}

	log.Infof("All %d results sent to client", resultsReceived)
}

// sendResponse writes the result back to the client in chunks
func sendResponse(proto *protocol.Protocol, queueID int32, result []byte) error {
	totalSize := len(result)

	if totalSize == 0 {
		log.Warningf("Empty result to send from queue %d", queueID)
		return proto.SendResultEOF(queueID)
	}

	// Calculate number of chunks
	totalChunks := int32((totalSize + resultChunkSize - 1) / resultChunkSize)

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

	}

	// Send EOF after all chunks for this queue
	return proto.SendResultEOF(queueID)
}

// createResultsCallback creates a callback function for consuming results from a specific queue
func createResultsCallback(resultChan chan ResultMessage, doneChan chan error, queueID int, channelPool *ChannelPool, channel *amqp.Channel) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		defer channelPool.Put(channel) // Return the consumer channel to the pool when done
		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					doneChan <- fmt.Errorf("channel closed unexpectedly for queue %d", queueID)
					return
				}

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
