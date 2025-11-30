package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	queryCount      = 4
	resultsWaitTime = 5 * time.Second
	ackTimeout      = 10 * time.Second
	maxRetries      = 3
)

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ServerPort string
	ServerIP   string
	ID         string
	BufferSize int
}

// Client manages connection to the server and file transfers
type Client struct {
	config         ClientConfig
	shutdown       chan struct{}
	conn           net.Conn
	protocol       *protocol.Protocol
	queryID        string
	pendingResults []int
}

// fileMetadata holds metadata about the file being transferred
type fileMetadata struct {
	fileType    protocol.FileType
	totalChunks int32
}

// NewClient creates a new Client instance
func NewClient(config ClientConfig) *Client {
	return &Client{
		config:         config,
		shutdown:       make(chan struct{}),
		pendingResults: []int{1, 2, 3, 4},
	}
}

// Start begins the client operations
func (c *Client) Start() error {
	if err := c.connectToServer(); err != nil {
		log.Errorf("Failed to connect to server: %v", err)
		return err
	}

	// Send message to server signaling the start of a query request
	if err := c.protocol.SendQueryRequest(); err != nil {
		log.Errorf("Failed to send query request: %v", err)
		return err
	}

	// Wait for Query Id
	if err := c.waitForQueryId(); err != nil {
		log.Errorf("Failed to receive QueryId: %v", err)
		return err
	}

	if err := c.TransferDataDirectory("/data"); err != nil {
		log.Errorf("Failed to transfer data: %v", err)
		return err
	}

	// Send final EOF after all directories are transferred
	c.protocol.SendFinalEOF()

	log.Info("Transfered all files succesfully, closing connection")
	c.stopServerConn()

	for {
		if err := c.tryReadResults(); err != nil {
			log.Errorf("Failed to read results: %v", err)
			return err
		}

		if len(c.pendingResults) == 0 {
			log.Infof("All %d results received successfully", queryCount)
			break
		}
	}

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

				if err := c.sendWithACK(func() error {
					return c.protocol.SendEOF(expected.fileType)
				}, fmt.Sprintf("EOF for %s", expected.fileType)); err != nil {
					log.Errorf("Failed to send EOF for fileType %s: %v", expected.fileType, err)
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
	totalChunks := math.Ceil(float64(fileInfo.Size()) / float64(c.config.BufferSize))
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
		rows, isEOF, err := c.readBatch(reader, c.config.BufferSize)
		if err != nil {
			return fmt.Errorf("failed to read batch: %w", err)
		}

		if len(rows) > 0 {
			// Convert [][]string to []string (CSV format)
			csvRows := make([]string, len(rows))
			for i, row := range rows {
				csvRows[i] = joinCSVRow(row)
			}

			// Convert client ID string to [8]byte
			clientID, err := protocol.ClientIDFromString(c.config.ID)
			if err != nil {
				return fmt.Errorf("invalid client ID: %w", err)
			}

			batchMsg := &protocol.BatchMessage{
				ClientID:     clientID,
				FileType:     metadata.fileType,
				CurrentChunk: currentChunk,
				TotalChunks:  metadata.totalChunks,
				CSVRows:      csvRows,
			}

			if err := c.sendWithACK(func() error {
				return c.protocol.SendBatch(batchMsg)
			}, fmt.Sprintf("chunk %d/%d (FileType: %s)", currentChunk, metadata.totalChunks, metadata.fileType)); err != nil {
				return fmt.Errorf("failed to send batch %d: %w", currentChunk, err)
			}

			log.Infof("Sent chunk %d/%d (FileType: %s)",
				currentChunk, metadata.totalChunks, metadata.fileType)
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
	var result strings.Builder
	for i, field := range row {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(field)
	}
	return result.String()
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

func (c *Client) reconnect() error {
	err := c.connectToServer()
	if err != nil {
		return err
	}
	c.protocol.SendResumeRequest(c.queryID)
	err = c.waitForACK()
	return err
}

// stopServerConn stops connection with the server
func (c *Client) stopServerConn() {
	if conn := c.conn; conn != nil {
		conn.Close()
		c.conn = nil
	}
	c.protocol = nil
}

// sendWithACK sends a message and waits for ACK with retries on timeout or connection failure
func (c *Client) sendWithACK(sendFunc func() error, msgDesc string) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Check if connection is still valid, reconnect if needed
		if err := c.ensureConnected(); err != nil {
			log.Warningf("Connection failed for %s: %v", msgDesc, err)
			if attempt < maxRetries {
				log.Infof("Attempting to reconnect (attempt %d/%d)", attempt+1, maxRetries)

				time.Sleep(time.Second)
				continue
			}
			return fmt.Errorf("failed to connect for %s after %d attempts: %w", msgDesc, maxRetries, err)
		}

		// Send the message
		if err := sendFunc(); err != nil {
			log.Warningf("Failed to send %s: %v", msgDesc, err)

			// Connection likely broken, mark for reconnection
			c.conn = nil
			c.protocol = nil
			if attempt < maxRetries {
				log.Infof("Retrying %s (attempt %d/%d)", msgDesc, attempt+1, maxRetries)
				time.Sleep(time.Second) // Brief delay before retry
				continue
			}
			return fmt.Errorf("failed to send %s after %d attempts: %w", msgDesc, maxRetries, err)
		}

		log.Debugf("Sent %s (attempt %d/%d)", msgDesc, attempt, maxRetries)

		// Wait for ACK with timeout
		ackChan := make(chan error, 1)
		go func() {
			ackChan <- c.waitForACK()
		}()

		select {
		case err := <-ackChan:
			if err != nil {
				log.Warningf("Failed to receive ACK for %s: %v", msgDesc, err)
				// Connection likely broken, mark for reconnection

				c.protocol = nil
				c.conn = nil
				if attempt < maxRetries {
					log.Infof("Retrying %s (attempt %d/%d)", msgDesc, attempt+1, maxRetries)
					time.Sleep(time.Second) // Brief delay before retry
					continue
				}
				return fmt.Errorf("failed to receive ACK for %s after %d attempts: %w", msgDesc, maxRetries, err)
			}
			log.Debugf("Received ACK for %s", msgDesc)
			return nil

		case <-time.After(ackTimeout):
			log.Warningf("ACK timeout for %s (attempt %d/%d)", msgDesc, attempt, maxRetries)

			// Connection likely broken, mark for reconnection
			c.conn = nil
			c.protocol = nil
			if attempt < maxRetries {
				log.Infof("Retrying %s (attempt %d/%d)", msgDesc, attempt+1, maxRetries)
				time.Sleep(time.Second) // Brief delay before retry
				continue
			}
			return fmt.Errorf("ACK timeout for %s after %d attempts", msgDesc, maxRetries)
		}
	}

	return fmt.Errorf("failed to send %s after %d retries", msgDesc, maxRetries)
}

// ensureConnected checks if connected, and reconnects if not
func (c *Client) ensureConnected() error {
	if c.conn != nil && c.protocol != nil {
		return nil
	}

	log.Infof("Reconnecting to server...")
	return c.reconnect()
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

// waitForQueryId waits for an acknowledgment from the server
func (c *Client) waitForQueryId() error {
	msgType, data, err := c.protocol.ReceiveMessage()
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	switch msgType {
	case protocol.MessageTypeQueryId:
		queryID := data.(*protocol.QueryIdMessage).QueryID
		log.Infof("Received query ID: %s", queryID)
		c.queryID = queryID
	default:
		return fmt.Errorf("expected QueryId, got message type: 0x%02x", msgType)
	}
	return nil
}

// tryReadResults stablished connection with the server
// and reads the results from it
func (c *Client) tryReadResults() error {
	for {
		// Establish connection with the server
		log.Info("Connecting with server to request results...")
		if err := c.connectToServer(); err != nil {
			log.Errorf("Failed to connect to server: %v", err)
			// TODO: consider `continue` here
			// with a max number of retries
			return err
		}

		// Send results request with queryId
		if err := c.protocol.SendResultsRequest(c.queryID, c.pendingResults); err != nil {
			log.Errorf("Failed to send results request: %v", err)
			// TODO: consider `continue` here
			// with a max number of retries
			return err
		}
		log.Debugf("Sent results request for pending results: ", c.pendingResults)

		msgType, _, err := c.protocol.ReceiveMessage()
		if err != nil {
			return fmt.Errorf("failed to receive message: %w", err)
		}

		switch msgType {
		case protocol.MessageTypeResultsPending:
			log.Info("Request handler is still processing results")
			c.stopServerConn()
			time.Sleep(resultsWaitTime)
			continue
		case protocol.MessageTypeResultsReady:
			log.Info("Request handler ready to send results")
		default:
			// Something went wrong with the client, abort connection
			c.stopServerConn()
			return fmt.Errorf("unexpected message type while reading results: 0x%02x", msgType)
		}

		err = c.readResults()
		if err != nil {
			log.Errorf("failed to read results: %w", err)
			if c.conn != nil {
				c.stopServerConn()
			}
			// TODO: implement max retries to prevent infinite loop
		}
		c.stopServerConn()
		return nil
	}
}

// readResults reads and processes results from the server
func (c *Client) readResults() error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	log.Info("Waiting for results from server...")

	resultsReceived := 0
	resultBuffers := make(map[int32][]byte)

	for resultsReceived < len(c.pendingResults) {
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
					resultsReceived, queryCount, eofMsg.QueueID, len(result))

				if err := c.processResult(eofMsg.QueueID, result); err != nil {
					log.Errorf("Failed to process result from queue %d: %v", eofMsg.QueueID, err)
				}

				// Remove queue from pending results
				for i, v := range c.pendingResults {
					if v == int(eofMsg.QueueID) {
						c.pendingResults = append(c.pendingResults[:i], c.pendingResults[i+1:]...)
						break
					}
				}

				delete(resultBuffers, eofMsg.QueueID)
			}

		default:
			return fmt.Errorf("unexpected message type while reading results: 0x%02x", msgType)
		}
	}

	return nil
}

func (c *Client) processResult(queueID int32, data []byte) error {
	// Use /results directory (mounted volume)
	resultsDir := fmt.Sprintf("/results/client_%s", c.config.ID)

	// Create directory if it doesn't exist (though volume should exist)
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return fmt.Errorf("failed to create results directory: %v", err)
	}

	queryNum := queueID
	filename := filepath.Join(resultsDir, fmt.Sprintf("query_%d.csv", queryNum))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filename, err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write data to %s: %v", filename, err)
	}

	log.Infof("Successfully saved result for Query %d to %s (%d bytes)",
		queryNum, filename, len(data))
	return nil
}
