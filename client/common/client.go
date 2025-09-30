package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	defaultBatchSize     = 8 * 1024 // 8KB
	expectedResultsCount = 1
)

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ServerPort string
	ServerIP   string
}

// Client manages connection to the server and file transfers
type Client struct {
	config   ClientConfig
	shutdown chan struct{}
	conn     net.Conn
}

// fileMetadata holds metadata about the file being transferred
type fileMetadata struct {
	fileType    int
	fileHash    string
	totalChunks int
}

// NewClient creates a new Client instance
func NewClient(config ClientConfig) *Client {
	return &Client{
		config:   config,
		shutdown: make(chan struct{}),
	}
}

// Start begins the client operations
func (c *Client) Start() error {
	err := c.connectToServer()
	if err != nil {
		log.Errorf("Failed to connect to server: %v", err)
		return err
	}

	if err := c.TransferCSVFile("/data/transactions_test.csv"); err != nil {
		log.Errorf("Failed to transfer directory: %v", err)
		return err
	}

	log.Info("All operations completed successfully")
	c.readResults()
	<-c.shutdown
	return nil
}

// Stop gracefully shuts down the client
func (c *Client) Stop() {
	log.Info("Shutting down client...")

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Errorf("Error closing connection: %v", err)
		}
		c.conn = nil
	}

	close(c.shutdown)
	log.Info("Client shutdown complete")
}

// TransferCSVFile reads and transfers a CSV file in batches
func (c *Client) TransferCSVFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	reader := csv.NewReader(file)

	// Skip header row
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	metadata := c.calculateFileMetadata(fileInfo)

	return c.transferFileInBatches(reader, metadata)
}

// calculateFileMetadata extracts and calculates file metadata
func (c *Client) calculateFileMetadata(fileInfo os.FileInfo) fileMetadata {
	totalChunks := int(fileInfo.Size()) / defaultBatchSize
	if totalChunks == 0 {
		totalChunks = 1
	}

	return fileMetadata{
		fileType:    0,               // TODO: CREATE ENUM
		fileHash:    fileInfo.Name(), // TODO: CALCULATE FILE HASH
		totalChunks: totalChunks,
	}
}

// transferFileInBatches reads and sends the file in batches
func (c *Client) transferFileInBatches(reader *csv.Reader, metadata fileMetadata) error {
	currentChunk := 1

	for {
		rows, isEOF, err := c.readBatch(reader, defaultBatchSize)
		if err != nil {
			return fmt.Errorf("failed to read batch: %w", err)
		}

		if len(rows) > 0 {
			if err := c.sendCSVBatch(metadata, currentChunk, rows); err != nil {
				return fmt.Errorf("failed to send batch %d: %w", currentChunk, err)
			}
			log.Infof("Sent chunk %d/%d of file %s", currentChunk, metadata.totalChunks, metadata.fileHash)
		}

		if isEOF {
			break
		}

		currentChunk++
	}

	return nil
}

// readBatch reads a batch of CSV rows up to the specified size
func (c *Client) readBatch(reader *csv.Reader, batchSize int) ([][]string, bool, error) {
	var rows [][]string
	bytesRead := 0

	for bytesRead < batchSize {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return rows, true, nil
			}
			return nil, false, fmt.Errorf("failed to read CSV record: %w", err)
		}

		rows = append(rows, record)
		bytesRead += calculateRecordSize(record)
	}

	return rows, false, nil
}

// calculateRecordSize calculates the approximate size of a CSV record
func calculateRecordSize(record []string) int {
	size := 0
	for _, field := range record {
		size += len(field)
	}
	return size
}

// sendCSVBatch serializes and sends a batch of CSV rows
func (c *Client) sendCSVBatch(metadata fileMetadata, currentChunk int, rows [][]string) error {
	csvBatchMsg := SerializeCSVBatch(
		metadata.fileType,
		metadata.fileHash,
		metadata.totalChunks,
		currentChunk,
		rows,
	)

	return c.sendBatch(csvBatchMsg)
}

// connectToServer establishes a connection to the server
func (c *Client) connectToServer() error {
	if c.conn != nil {
		return nil // Already connected
	}

	addr := net.JoinHostPort(c.config.ServerIP, c.config.ServerPort)
	log.Infof("Connecting to server: %s", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.conn = conn
	log.Info("Successfully connected to server")
	return nil
}

// sendBatch sends a batch message to the server
func (c *Client) sendBatch(csvBatchMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	writer := bufio.NewWriter(c.conn)

	if _, err := writer.WriteString(csvBatchMsg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Send end-of-message marker
	if _, err := writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write end marker: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush message: %w", err)
	}

	log.Debug("Batch sent and flushed")
	return nil
}

// readResults reads and processes results from the server
func (c *Client) readResults() error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	log.Info("Waiting for results from server...")

	scanner := bufio.NewScanner(c.conn)
	// Read header
	if !scanner.Scan() {
		return fmt.Errorf("failed to read header")
	}

	result := []string{}
	for i := 0; i < expectedResultsCount; i++ {
		for scanner.Scan() {
			row := scanner.Text()
			if row == "" {
				break
			}
			result = append(result, row)
		}
		result := strings.Join(result, "\n")

		log.Infof("Result %d/%d received - Length: %d bytes - Message: %s",
			i+1, expectedResultsCount, len(result), result)
	}

	log.Infof("All results received (%d/%d)", expectedResultsCount, expectedResultsCount)
	return nil
}
