package common

import (
	"encoding/csv"
	"net"
	"os"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	Port string
	Ip   string
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

func (m *Client) TransferCSVFile(path string) error {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Calculate the amount of file chunks
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Msg header components
	fileType := 0          // TODO: CREATE ENUM
	fileHash := ""         // TODO: CALCULATE FILE HASH
	batchSize := 1024 * 32 // 32KB // TODO: SET THIS IN CONFIG
	totalChunks := fileInfo.Size() / int64(batchSize)

	reader := csv.NewReader(file)
	currentChunk := int64(0)
	for {
		rows, isEof, err := readRows(reader, batchSize)
		if err != nil {
			return err
		}

		csvBatchMsg, err := SerializeCSVBatch(fileType, fileHash, totalChunks, currentChunk, rows)

		// TODO: REPLACE THIS LOG MESSAGE WITH ACTUAL TRANSFER
		log.Infof("Sending msg: %s", csvBatchMsg)

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
		log.Info("File transferred succesfully: ", entry.Name())
	}

	return nil
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
