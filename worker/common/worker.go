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

func (w *Worker) ConnectToServer() error {
	conn, err := net.Dial("tcp", w.config.MiddlewareAddress)
	if err != nil {
		return err
	}

	// Send connection message to server
	// TODO: send connection message with
	// - listener address
	// - subcription topics
	_, port, _ := net.SplitHostPort(w.listener.Addr().String())
	connMsg := "WORKER_CONN," + w.config.Ip + ":" + port
	_, err = conn.Write([]byte(connMsg)) // TODO: short write?
	if err != nil {
		return err
	}

	conn.Close()
	return nil
}

func (w *Worker) ListenToMiddleware() error {
	for {
		select {
		case <-w.shutdown:
			return nil
		default:
			// continue execution
		}

		conn, err := w.listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %s", err)
			return err
		}

		log.Infof("Received message from server")
		// TODO: after this we should read message from the server and process it

		conn.Close()
	}
}

func (w *Worker) Start() error {
	// TODO: create listener inside of `NewWorker`?
	listener, err := net.Listen("tcp", "")
	if err != nil {
		log.Errorf("Error starting worker: %s", err)
	}
	defer listener.Close()

	w.listener = listener
	log.Infof("Worker listening on %s", w.listener.Addr().String())

	// Connect to the server
	err = w.ConnectToServer()
	if err != nil {
		log.Errorf("Error connecting to server: %s", err)
		return err
	}

	// Receibe messages from the server in a separate goroutine
	go func() {
		// TODO: thread safe?
		w.ListenToMiddleware()
	}()

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
