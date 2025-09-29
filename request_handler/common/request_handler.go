package common

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type RequestHandlerConfig struct {
	Port          string
	Ip            string
	MiddlewareUrl string
}

type RequestHandler struct {
	Config      RequestHandlerConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

// BatchMessage represents the parsed message structure
type BatchMessage struct {
	FileType     string
	FileHash     string
	CurrentChunk int
	TotalChunks  int
	CsvRows      []string
}

func NewRequestHandler(config RequestHandlerConfig) *RequestHandler {
	request_handler := &RequestHandler{
		Config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return request_handler
}

func (m *RequestHandler) Start() error {
	// Create listener
	addr := net.JoinHostPort(m.Config.Ip, m.Config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	m.listener = listener
	log.Infof("RequestHandler listening on %s", addr)

	// Accept connections in a goroutine
	go m.acceptConnections()

	// Wait for shutdown signal
	<-m.shutdown
	return nil
}

func (m *RequestHandler) acceptConnections() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.shutdown:
				// Shutdown requested, exit gracefully
				return
			default:
				log.Errorf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle each connection in a separate goroutine
		go m.handleConnection(conn)
	}
}

func (m *RequestHandler) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Infof("New connection from %s", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)

	// Read the first line (message header)
	if !scanner.Scan() {
		log.Errorf("Failed to read header from %s", conn.RemoteAddr())
		return
	}

	header := scanner.Text()

	// Parse header: TRANSFER;<FILE_TYPE>;<FILE_HASH>;<CURRENT_CHUNK>;<TOTAL_CHUNKS>
	message, err := m.parseHeader(header)
	if err != nil {
		log.Errorf("Failed to parse header: %v", err)
		return
	}

	// Read CSV rows
	for scanner.Scan() {
		row := scanner.Text()
		if row == "" {
			break // End of message
		}
		message.CsvRows = append(message.CsvRows, row)
	}

	if err := scanner.Err(); err != nil {
		log.Errorf("Error reading from connection: %v", err)
		return
	}

	log.Infof("Received batch: %s (chunk %d/%d) with %d rows",
		message.FileHash, message.CurrentChunk, message.TotalChunks, len(message.CsvRows))

	// Send batch directly to the queue
	rabbit_conn, err := amqp.Dial(m.Config.MiddlewareUrl)
	channel, err := rabbit_conn.Channel()
	transactions_queue := middleware.NewMessageMiddlewareQueue("transactions", channel)
	finalMessage := strings.Join(message.CsvRows, "\n")
	transactions_queue.Send([]byte(finalMessage))
	log.Infof("Successfully forwarded batch to queue")
}

func (m *RequestHandler) parseHeader(header string) (*BatchMessage, error) {
	parts := strings.Split(header, ";")
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid header format: expected 5 parts, got %d", len(parts))
	}

	if parts[0] != "TRANSFER" {
		return nil, fmt.Errorf("invalid message type: expected TRANSFER, got %s", parts[0])
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
		CsvRows:      make([]string, 0),
	}, nil
}

func (m *RequestHandler) Stop() {
	log.Info("Shutting down request_handler...")

	// Send stop signal to running loop
	close(m.shutdown)

	if m.listener != nil {
		m.listener.Close()
	}

	log.Info("RequestHandler shut down complete.")
}
