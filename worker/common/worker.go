package common

import (
	"errors"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	filter "github.com/fiuba-distribuidos-2C2025/tp1/worker/common/filterer"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type WorkerConfig struct {
	MiddlewareUrl   string
	InputQueue      string
	OutputQueue     string
	OutputReceivers int
	WorkerJob       string
	ID              string
}

type Worker struct {
	config   WorkerConfig
	shutdown chan struct{}
	conn     *amqp.Connection
	channel  *amqp.Channel
}

func NewWorker(config WorkerConfig) (*Worker, error) {
	log.Infof("Connecting to RabbitMQ at %s ...", config.MiddlewareUrl)

	conn, err := dialWithRetry(config.MiddlewareUrl, 10, time.Second)
	if err != nil {
		log.Errorf("Failed to connect to RabbitMQ: %v", err)
		return nil, err
	}
	log.Infof("Connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		log.Errorf("Failed to open a rabbitmq channel: %v", err)
		return nil, err
	}
	log.Infof("RabbitMQ channel opened")

	return &Worker{
		config:   config,
		shutdown: make(chan struct{}, 1),
		conn:     conn,
		channel:  ch,
	}, nil
}

// small retry helper
func dialWithRetry(url string, attempts int, delay time.Duration) (*amqp.Connection, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(delay)
	}
	return nil, lastErr
}

func (w *Worker) Start() error {
	log.Infof("Worker started!")

	// Start cleanup
	// TODO: We should use the middleware interface.
	defer func() {
		_ = w.channel.Close()
		_ = w.conn.Close()
	}()

	log.Infof("Declaring queues...")

	inQueueResponseChan := make(chan string)
	inQueue := middleware.NewMessageMiddlewareExchange(w.config.InputQueue, []string{w.config.ID}, w.channel)

	switch w.config.WorkerJob {
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan))
	case "AMOUNT_FILTER":
		log.Info("Starting AMOUNT_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByAmountFilterCallbackWithOutput(inQueueResponseChan))
	default:
		log.Error("Unknown worker job")
		return errors.New("Unknown worker job")
	}

	outputQueues := make([]*middleware.MessageMiddlewareExchange, w.config.OutputReceivers)
	outputBroadcast := middleware.NewMessageMiddlewareExchange(w.config.OutputQueue, []string{"#"}, w.channel)
	for id := 0; id <= w.config.OutputReceivers-1; id++ {
		outputQueues[id] = middleware.NewMessageMiddlewareExchange(w.config.OutputQueue, []string{string(rune(id))}, w.channel)
	}

	for {
		idx := 0
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inQueueResponseChan:
			if msg == "EOF" {
				log.Infof("Broadcasting EOF")
				outputBroadcast.Send([]byte("EOF"))
				return nil
			}
			receiver := idx % w.config.OutputReceivers
			// TODO: forward in batches
			log.Infof("Forwarding message: %s to worker %i", msg, receiver+1)
			outputQueues[receiver].Send([]byte(msg))
			idx += 1
		}

		// TODO: know when input queue is finished!
	}
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
