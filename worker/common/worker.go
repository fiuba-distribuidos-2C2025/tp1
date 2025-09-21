package common

import (
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type WorkerConfig struct {
	MiddlewareAddress string
	Ip                string
}

type Worker struct {
	config   WorkerConfig
	listener net.Listener
	shutdown chan struct{}
}

func NewWorker(config WorkerConfig) *Worker {
	worker := &Worker{
		config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return worker
}

func (w *Worker) Start() error {
	// Wait for shutdown signal
	<-w.shutdown
	return nil
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	if w.listener != nil {
		w.listener.Close()
	}

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
