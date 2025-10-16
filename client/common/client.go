package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/fiuba-distribuidos-2C2025/tp1/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	defaultBatchSize = 10 * 1024 * 1024 // 10MB
)

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ServerPort string
	ServerIP   string
	ID         string
}

// Client manages connection to the server and file transfers
type Client struct {
	config   ClientConfig
	shutdown chan struct{}
	conn     net.Conn
	protocol *protocol.Protocol
}

// fileMetadata holds metadata about the file being transferred
type fileMetadata struct {
	fileType    protocol.FileType
	totalChunks int32
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
	if err := c.connectToServer(); err != nil {
		log.Errorf("Failed to connect to server: %v", err)
		return err
	}

	if err := c.TransferDataDirectory("/data"); err != nil {
		log.Errorf("Failed to transfer data: %v", err)
		return err
	}

	// Send final EOF after all directories are transferred
	if err := c.protocol.SendFinalEOF(); err != nil {
		log.Errorf("Failed to send final EOF: %v", err)
		return err
	}

	// Wait for ACK
	if err := c.waitForACK(); err != nil {
		log.Errorf("Failed to receive final EOF ACK: %v", err)
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

// TransferDataDirectory iterates over subdirectories and assigns file types
func (c *Client) TransferDataDirectory(dataPath string) error {
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	// Define expected directory structure
	expectedDirs := []struct {
		name     string
		fileType protocol.FileType
	}{
		{"transactions", protocol.FileTypeTransactions},
		{"transactions_items", protocol.FileTypeTransactionItems},
		{"stores", protocol.FileTypeStores},
		{"menu_items", protocol.FileTypeMenuItems},
		{"users", protocol.FileTypeUsers},
	}

	processedCount := 0

	for _, expected := range expectedDirs {
		found := false
		for _, entry := range entries {
			if entry.Name() == expected.name && entry.IsDir() {
				found = true
				dirPath := filepath.Join(dataPath, entry.Name())
				log.Infof("Processing directory: %s (FileType: %s)", entry.Name(), expected.fileType)

				if err := c.TransferCSVFolder(dirPath, expected.fileType); err != nil {
					log.Errorf("Failed to transfer CSVs from directory %s: %v", entry.Name(), err)
					return err
				}

				if err := c.protocol.SendEOF(expected.fileType); err != nil {
					log.Errorf("Failed to send EOF for fileType %s: %v", expected.fileType, err)
					return err
				}

				if err := c.waitForACK(); err != nil {
					log.Errorf("Failed to receive EOF ACK for fileType %s: %v", expected.fileType, err)
					return err
				}

				log.Infof("Completed directory %s (FileType: %s)", entry.Name(), expected.fileType)
				processedCount++
				break
			}
		}

		if !found {
			log.Warningf("Expected directory not found: %s", expected.name)
		}
	}

	if processedCount == 0 {
		return fmt.Errorf("no expected subdirectories found in: %s", dataPath)
	}

	log.Infof("Processed %d directories successfully", processedCount)
	return nil
}

// TransferCSVFolder finds and transfers all CSV files in a directory
func (c *Client) TransferCSVFolder(folderPath string, fileType protocol.FileType) error {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	csvFiles := []os.DirEntry{}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".csv" {
			csvFiles = append(csvFiles, entry)
		}
	}

	if len(csvFiles) == 0 {
		log.Warningf("No CSV files found in %s", folderPath)
		return nil
	}

	for i, entry := range csvFiles {
		filePath := filepath.Join(folderPath, entry.Name())
		log.Infof("Transferring file %d/%d: %s (FileType: %s)",
			i+1, len(csvFiles), entry.Name(), fileType)

		if err := c.TransferCSVFile(filePath, fileType); err != nil {
			return fmt.Errorf("failed to transfer file %s: %w", filePath, err)
		}

		log.Infof("Successfully transferred file %d/%d: %s", i+1, len(csvFiles), entry.Name())
	}

	log.Infof("All %d CSV files transferred successfully from %s", len(csvFiles), folderPath)
	return nil
}

// TransferCSVFile reads and transfers a CSV file in batches
func (c *Client) TransferCSVFile(path string, fileType protocol.FileType) error {
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

	metadata := c.calculateFileMetadata(fileInfo, fileType)
	return c.transferFileInBatches(reader, metadata)
}

// calculateFileMetadata extracts and calculates file metadata
func (c *Client) calculateFileMetadata(fileInfo os.FileInfo, fileType protocol.FileType) fileMetadata {
	totalChunks := math.Ceil(float64(fileInfo.Size()) / float64(defaultBatchSize))
	if totalChunks == 0 {
		totalChunks = 1
	}

	return fileMetadata{
		fileType:    fileType,
		totalChunks: int32(totalChunks),
	}
}

// transferFileInBatches reads and sends the file in batches
func (c *Client) transferFileInBatches(reader *csv.Reader, metadata fileMetadata) error {
	currentChunk := int32(1)

	for {
		rows, isEOF, err := c.readBatch(reader, defaultBatchSize)
		if err != nil {
			return fmt.Errorf("failed to read batch: %w", err)
		}

		if len(rows) > 0 {
			// Convert [][]string to []string (CSV format)
			csvRows := make([]string, len(rows))
			for i, row := range rows {
				csvRows[i] = joinCSVRow(row)
			}

			clientID, _ := strconv.Atoi(c.config.ID) // TODO: handle error
			uinclientID := uint16(clientID)
			batchMsg := &protocol.BatchMessage{
				ClientID:     uinclientID,
				FileType:     metadata.fileType,
				CurrentChunk: currentChunk,
				TotalChunks:  metadata.totalChunks,
				CSVRows:      csvRows,
			}

			if err := c.protocol.SendBatch(batchMsg); err != nil {
				return fmt.Errorf("failed to send batch %d: %w", currentChunk, err)
			}

			log.Infof("Sent chunk %d/%d (FileType: %s)",
				currentChunk, metadata.totalChunks, metadata.fileType)

			// Wait for ACK
			if err := c.waitForACK(); err != nil {
				return fmt.Errorf("failed to receive ACK for chunk %d: %w", currentChunk, err)
			}
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
		size += len(field) + 1 // +1 for comma or newline
	}
	return size
}

// joinCSVRow joins a CSV row into a single string
func joinCSVRow(row []string) string {
	result := ""
	for i, field := range row {
		if i > 0 {
			result += ","
		}
		// Escape fields containing commas or quotes
		if containsSpecialChars(field) {
			result += "\"" + escapeQuotes(field) + "\""
		} else {
			result += field
		}
	}
	return result
}

// containsSpecialChars checks if a field needs quoting
func containsSpecialChars(field string) bool {
	for _, ch := range field {
		if ch == ',' || ch == '"' || ch == '\n' || ch == '\r' {
			return true
		}
	}
	return false
}

// escapeQuotes escapes quotes in a field
func escapeQuotes(field string) string {
	result := ""
	for _, ch := range field {
		if ch == '"' {
			result += "\"\""
		} else {
			result += string(ch)
		}
	}
	return result
}

// connectToServer establishes a connection to the server
func (c *Client) connectToServer() error {
	if c.conn != nil {
		return nil
	}

	addr := net.JoinHostPort(c.config.ServerIP, c.config.ServerPort)
	log.Infof("Connecting to server: %s", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.conn = conn
	c.protocol = protocol.NewProtocol(conn)
	log.Info("Successfully connected to server")
	return nil
}

// waitForACK waits for an acknowledgment from the server
func (c *Client) waitForACK() error {
	msgType, _, err := c.protocol.ReceiveMessage()
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	if msgType != protocol.MessageTypeACK {
		return fmt.Errorf("expected ACK, got message type: 0x%02x", msgType)
	}

	return nil
}

// readResults reads and processes results from the server
func (c *Client) readResults() error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	log.Info("Waiting for results from server...")

	resultsReceived := 0
	expectedResults := 4
	resultBuffers := make(map[int32][]byte)

	for resultsReceived < expectedResults {
		msgType, data, err := c.protocol.ReceiveMessage()
		if err != nil {
			return fmt.Errorf("failed to receive message: %w", err)
		}

		switch msgType {
		case protocol.MessageTypeResultChunk:
			chunk := data.(*protocol.ResultChunkMessage)
			log.Infof("Received result chunk %d/%d from queue %d (%d bytes)",
				chunk.CurrentChunk, chunk.TotalChunks, chunk.QueueID, len(chunk.Data))

			// Accumulate chunks
			resultBuffers[chunk.QueueID] = append(resultBuffers[chunk.QueueID], chunk.Data...)

		case protocol.MessageTypeResultEOF:
			eofMsg := data.(*protocol.ResultEOFMessage)
			resultsReceived++

			if result, ok := resultBuffers[eofMsg.QueueID]; ok {
				log.Infof("Result %d/%d received from queue %d - Total size: %d bytes",
					resultsReceived, expectedResults, eofMsg.QueueID, len(result))

				// Process result here (save to file, print, etc.)
				c.processResult(eofMsg.QueueID, result)

				// Clean up buffer
				delete(resultBuffers, eofMsg.QueueID)
			}

		default:
			return fmt.Errorf("unexpected message type while reading results: 0x%02x", msgType)
		}
	}

	log.Infof("All %d results received successfully", expectedResults)
	return nil
}

func (c *Client) processResult(queueID int32, data []byte) {
	// Use /results directory (mounted volume)
	resultsDir := fmt.Sprintf("/results/client_%s", c.config.ID)

	// Create directory if it doesn't exist (though volume should exist)
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		log.Errorf("Failed to create results directory: %v", err)
		return
	}

	queryNum := queueID
	filename := filepath.Join(resultsDir, fmt.Sprintf("query_%d.csv", queryNum))

	file, err := os.Create(filename)
	if err != nil {
		log.Errorf("Failed to create file %s: %v", filename, err)
		return
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		log.Errorf("Failed to write data to %s: %v", filename, err)
		return
	}

	log.Infof("Successfully saved result for Query %d to %s (%d bytes)",
		queryNum, filename, len(data))
}
