package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	defaultBatchSize     = 10 * 1024 * 1024 // 10MB
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

	if err := c.TransferDataDirectory("/data"); err != nil {
		log.Errorf("Failed to transfer data: %v", err)
		return err
	}

	// Send final EOF after all directories are transferred
	if err := c.sendFinalEOF(); err != nil {
		log.Errorf("Failed to send final EOF: %v", err)
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
	// Read directory contents
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	fileType := 0
	processedDirs := 0

	for _, entry := range entries {
		dirPath := filepath.Join(dataPath, entry.Name())
		log.Infof("Processing directory: %s (FileType: %d)", entry.Name(), fileType)

		// Transfer all CSV files in this directory with the current fileType
		if err := c.TransferCSVFolder(dirPath, fileType); err != nil {
			log.Errorf("Failed to transfer CSVs from directory %s: %v", entry.Name(), err)
			return err
		}

		// Send EOF after completing this directory/fileType
		if err := c.sendEOFForFileType(fileType); err != nil {
			log.Errorf("Failed to send EOF for fileType %d: %v", fileType, err)
			return err
		}

		log.Infof("Completed directory %s (FileType: %d)", entry.Name(), fileType)

		processedDirs++
		fileType++
	}

	if processedDirs == 0 {
		return fmt.Errorf("no subdirectories found in: %s", dataPath)
	}

	log.Infof("Processed %d directories successfully", processedDirs)
	return nil
}

// TransferCSVFolder finds and transfers all CSV files in a directory with the given fileType
func (c *Client) TransferCSVFolder(folderPath string, fileType int) error {
	// Read directory contents
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for i, entry := range entries {
		filePath := filepath.Join(folderPath, entry.Name())
		log.Infof("Transferring file %d/%d: %s (FileType: %d)", i+1, len(entries), filePath, fileType)

		if err := c.TransferCSVFile(filePath, fileType); err != nil {
			return fmt.Errorf("failed to transfer file %s: %w", filePath, err)
		}

		log.Infof("Successfully transferred file %d/%d: %s", i+1, len(entries), filePath)
	}

	log.Infof("All %d CSV files transferred successfully from %s", len(entries), folderPath)
	return nil
}

// TransferCSVFile reads and transfers a CSV file in batches
func (c *Client) TransferCSVFile(path string, fileType int) error {
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
func (c *Client) calculateFileMetadata(fileInfo os.FileInfo, fileType int) fileMetadata {
	totalChunks := math.Ceil(float64(fileInfo.Size()) / float64(defaultBatchSize))
	if totalChunks == 0 {
		totalChunks = 1
	}

	return fileMetadata{
		fileType:    fileType,
		fileHash:    fileInfo.Name(), // TODO: CALCULATE FILE HASH
		totalChunks: int(totalChunks),
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
			batchMessage := SerializeCSVBatch(metadata.fileType, metadata.fileHash, metadata.totalChunks, currentChunk, rows)
			if err := c.sendBatch(batchMessage); err != nil {
				return fmt.Errorf("failed to send batch %d: %w", currentChunk, err)
			}
			log.Infof("Sent chunk %d/%d of file %s (FileType: %d)", currentChunk, metadata.totalChunks, metadata.fileHash, metadata.fileType)
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

// sendBatch sends a batch message to the server and waits for ACK
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

	log.Debug("Batch sent and flushed, waiting for ACK...")

	// Wait for ACK from server
	if err := c.waitForACK(); err != nil {
		return fmt.Errorf("failed to receive ACK: %w", err)
	}

	log.Debug("ACK received")
	return nil
}

// sendEOFForFileType sends an EOF message for a specific file type to the server and waits for ACK
func (c *Client) sendEOFForFileType(fileType int) error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	writer := bufio.NewWriter(c.conn)

	// Send EOF message with file type
	eofMsg := fmt.Sprintf("EOF;%d\n", fileType)
	if _, err := writer.WriteString(eofMsg); err != nil {
		return fmt.Errorf("failed to write EOF for fileType %d: %w", fileType, err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush EOF for fileType %d: %w", fileType, err)
	}

	log.Infof("EOF sent to server for fileType %d, waiting for ACK...", fileType)

	// Wait for ACK from server
	if err := c.waitForACK(); err != nil {
		return fmt.Errorf("failed to receive EOF ACK for fileType %d: %w", fileType, err)
	}

	log.Infof("EOF ACK received for fileType %d", fileType)
	return nil
}

// sendFinalEOF sends a final EOF message to indicate all transfers are complete
func (c *Client) sendFinalEOF() error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	writer := bufio.NewWriter(c.conn)

	// Send final EOF message
	if _, err := writer.WriteString("FINAL_EOF\n"); err != nil {
		return fmt.Errorf("failed to write FINAL_EOF: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush FINAL_EOF: %w", err)
	}

	log.Info("FINAL_EOF sent to server, waiting for ACK...")

	// Wait for ACK from server
	if err := c.waitForACK(); err != nil {
		return fmt.Errorf("failed to receive FINAL_EOF ACK: %w", err)
	}

	log.Info("FINAL_EOF ACK received")
	return nil
}

// waitForACK waits for an acknowledgment from the server
func (c *Client) waitForACK() error {
	scanner := bufio.NewScanner(c.conn)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanner error while waiting for ACK: %w", err)
		}
		return fmt.Errorf("connection closed while waiting for ACK")
	}

	ack := scanner.Text()
	if ack != "ACK" {
		return fmt.Errorf("expected ACK, got: %s", ack)
	}

	return nil
}

// readResults reads and processes results from the server
func (c *Client) readResults() error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	log.Info("Waiting for results from server...")

	scanner := bufio.NewScanner(c.conn)

	result := []string{}
	for {
		for scanner.Scan() {
			row := scanner.Text()
			if row == "" {
				break
			}
			result = append(result, row)
		}
		resultStr := strings.Join(result, "\n")

		log.Infof("Result received - Length: %d bytes - Message: %s",
			len(resultStr), resultStr)

		// TODO: Eventually ack here
	}
}
