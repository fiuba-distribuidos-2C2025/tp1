package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"net"
	"os"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ServerPort    string
	ServerIp      string
	MiddlewareUrl string
}

type Client struct {
	config      ClientConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return client
}

func readRows(reader *csv.Reader, batchSize int) ([][]string, bool, error) {
	var rows [][]string
	isEof := false
	bytesRead := 0
	for {
		if bytesRead >= batchSize {
			break
		}
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				isEof = true
				break
			}
			return nil, false, err
		}
		rows = append(rows, record)
		// TODO: inefficient?
		for _, field := range record {
			bytesRead += len(field)
		}
	}
	return rows, isEof, nil
}

func (m *Client) sendBatch(csvBatchMsg string) error {
	// Connect to the request handler5
	log.Infof("Connecting to server: %s, %s", m.config.ServerIp, m.config.ServerPort)
	addr := net.JoinHostPort(m.config.ServerIp, m.config.ServerPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer conn.Close()

	// Send the batch message
	writer := bufio.NewWriter(conn)
	_, err = writer.WriteString(csvBatchMsg)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Flush to ensure all data is sent
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush message: %w", err)
	}

	return nil
}

func (m *Client) TransferCSVFile(path string) error {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read() // Ignore header row
	if err != nil {
		return err
	}

	// Calculate the amount of file chunks
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Msg header components
	fileType := 0               // TODO: CREATE ENUM
	fileHash := fileInfo.Name() // TODO: CALCULATE FILE HASH
	batchSize := 1024 * 8       // 8KB // TODO: SET THIS IN CONFIG
	totalChunks := int(fileInfo.Size()) / batchSize
	if totalChunks == 0 {
		totalChunks = 1
	}

	currentChunk := 1
	for {
		rows, isEof, err := readRows(reader, batchSize)
		if err != nil {
			return err
		}

		// Serialize and send the batch
		csvBatchMsg := SerializeCSVBatch(fileType, fileHash, totalChunks, currentChunk, rows)

		log.Infof("Sending chunk %d/%d of file %s", currentChunk, totalChunks, fileHash)

		err = m.sendBatch(csvBatchMsg)
		if err != nil {
			log.Errorf("Failed to send batch: %v", err)
			return err
		}

		if isEof {
			break
		}
		currentChunk++
	}

	return nil
}

func (m *Client) TransferDirectory(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		log.Errorf("Could not read data directory: %s", err)
		return err
	}

	for _, entry := range entries {
		// If the entry is a directory, call recursively
		if entry.IsDir() {
			subDir := directory + "/" + entry.Name()
			m.TransferDirectory(subDir)
			continue
		}

		// If the entry is a file, transfer it
		log.Info("Transferring file: ", entry.Name())
		path := directory + "/" + entry.Name()
		err = m.TransferCSVFile(path)
		if err != nil {
			log.Errorf("Could not transfer file %s: %s", entry.Name(), err)
			return err
		}
		log.Info("File transferred successfully: ", entry.Name())
	}

	rabbitConn, err := amqp.Dial(m.config.MiddlewareUrl)
	if err != nil {
		log.Errorf("Failed to connect to middleware: %v", err)
		return err
	}
	defer rabbitConn.Close()
	channel, err := rabbitConn.Channel()
	if err != nil {
		log.Errorf("Failed to create channel: %v", err)
		return err
	}
	defer channel.Close()
	resultsQueue := middleware.NewMessageMiddlewareQueue("results", channel)
	resultsQueue.StartConsuming(ResultsCallback())

	return nil
}

func ResultsCallback() func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for response...")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Info("Channel closed")
					return
				}
				log.Infof("Message received - Length: %d, Body: %s", len(msg.Body), string(msg.Body))
				msg.Ack(false)
			}
		}
	}
}

func (m *Client) Start() error {
	// Send initial files to workers
	datasetDir := "/data"
	m.TransferDirectory(datasetDir)

	// Wait for shutdown signal
	<-m.shutdown
	return nil
}

func (m *Client) Stop() {
	log.Info("Shutting down client...")
	// Send stop signal to running loop
	close(m.shutdown)
	if m.listener != nil {
		m.listener.Close()
	}
	log.Info("Client shut down complete.")
}
