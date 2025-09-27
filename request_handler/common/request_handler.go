package common

import (
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type RequestHandlerConfig struct {
	Port string
	Ip   string
}

type RequestHandler struct {
	config      RequestHandlerConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewRequestHandler(config RequestHandlerConfig) *RequestHandler {
	request_handler := &RequestHandler{
		config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return request_handler
}

func (m *RequestHandler) Start() error {
	// Wait for shutdown signal
	<-m.shutdown
	return nil

	// Recibo batch de datos del cliente
	// Manda ese batch directo a la cola]
	// Actualmente el mensaje que recibe es del tipo
	// TRANSFER;<FILE_TYPE>;<FILE_HASH>;<CURRENT_CHUNK>;<TOTAL_CHUNKS>
	// csv_row0
	// csv_row1
	// ...
	// csv_rown
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
