package common

import (
	"net"

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

func (m *Client) Start() error {
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
