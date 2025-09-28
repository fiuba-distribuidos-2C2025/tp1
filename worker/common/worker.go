package common

import (
	"context"
	"errors"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	filter "github.com/fiuba-distribuidos-2C2025/tp1/worker/common/filterer"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type WorkerConfig struct {
	MiddlewareUrl string
	InputQueue    string
	OutputQueue   string
	WorkerJob     string
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
	defer func() {
		_ = w.channel.Close()
		_ = w.conn.Close()
	}()

	log.Infof("Declaring queues...")

	inQueueResponseChan := make(chan string)
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue, w.channel)

	switch w.config.WorkerJob {
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan))
	default:
		log.Error("Unknown worker job")
		return errors.New("Unknown worker job")
	}

	outQueue := middleware.NewMessageMiddlewareQueue(w.config.OutputQueue, w.channel)
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inQueueResponseChan:
			log.Infof("Forwarding message: %s", msg)
			outQueue.Send([]byte(msg))
		}

		// TODO: know when input queue is finished!
	}
}

func (w *Worker) forwardMsg(msg string) error {
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := w.channel.PublishWithContext(
		pubCtx,
		"", w.config.OutputQueue,
		false, false,
		amqp.Publishing{ContentType: "text/plain", DeliveryMode: amqp.Persistent, Body: []byte(msg)},
	); err != nil {
		return err
	}

	log.Infof("Forwarded message: %s", msg)
	return nil
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
