package common

import (
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type MiddlewareConfig struct {
	Port string
	Ip   string
}

type Middleware struct {
	config      MiddlewareConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewMiddleware(config MiddlewareConfig) *Middleware {
	middleware := &Middleware{
		config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return middleware
}

func (m *Middleware) Start() error {
	// Wait for shutdown signal
	<-m.shutdown
	return nil
}

func (m *Middleware) Stop() {
	log.Info("Shutting down middleware...")

	// Send stop signal to running loop
	close(m.shutdown)

	if m.listener != nil {
		m.listener.Close()
	}

	log.Info("Middleware shut down complete.")
}
