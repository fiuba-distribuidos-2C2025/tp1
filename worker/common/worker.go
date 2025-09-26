package common

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type WorkerConfig struct {
	MiddlewareUrl string
	InputQueue    string
	OutputQueue   string
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

	inQueue, err := w.channel.QueueDeclare(
		w.config.InputQueue, false, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	log.Infof("Declared input queue %q", inQueue.Name)

	outQueue, err := w.channel.QueueDeclare(
		w.config.OutputQueue, false, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	log.Infof("Declared output queue %q", outQueue.Name)

	var total int64

	msgs, err := w.channel.Consume(
		inQueue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	log.Infof("Waiting for messages...")

	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg, ok := <-msgs:
			if !ok {
				log.Infof("Deliveries channel closed; shutting down")
				return nil
			}
			body := strings.TrimSpace(string(msg.Body))
			if strings.EqualFold(body, "fin") {
				if err := handleFinMsg(w.channel, outQueue, &total); err != nil {
					return err
				}
				continue
			}
			n, parseErr := strconv.ParseInt(body, 10, 64)
			if parseErr != nil {
				log.Errorf("[worker] invalid integer %q: %v", body, parseErr)
				continue
			}

			log.Infof("Received number: %d", n)
			total += n
		}
	}
}

func handleFinMsg(ch *amqp.Channel, outQueue amqp.Queue, total *int64) error {
	payload := strconv.FormatInt(*total, 10)

	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(
		pubCtx,
		"", outQueue.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", DeliveryMode: amqp.Persistent, Body: []byte(payload)},
	); err != nil {
		return err
	}

	log.Infof("[worker] FIN received; published total=%d; resetting", *total)
	*total = 0
	return nil
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
