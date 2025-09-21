package common

import (
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

func (m *Client) TransferFile(path string) error {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("Could not open file %s: %s", path, err)
		return err
	}
	defer file.Close()

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
		err = m.TransferFile(path)
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
