package common

import (
	"errors"
	"strconv"
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
	OutputQueue     []string
	InputSenders    int
	OutputReceivers []string
	WorkerJob       string
	ID              int
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
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue+"_"+strconv.Itoa(w.config.ID), w.channel)
	log.Infof("Input queue declared: %s", w.config.InputQueue)

	neededEof := w.config.InputSenders
	switch w.config.WorkerJob {
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AMOUNT_FILTER":
		log.Info("Starting AMOUNT_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByAmountFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	default:
		log.Error("Unknown worker job")
		return errors.New("Unknown worker job")
	}

	outputQueues := make([][]*middleware.MessageMiddlewareQueue, len(w.config.OutputReceivers))
	for i := 0; i < len(w.config.OutputReceivers); i++ {
		outputQueueName := w.config.OutputQueue[i]
		outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
		if err != nil {
			log.Errorf("Error parsing output receivers: %s", err)
			return err
		}

		outputQueues[i] = make([]*middleware.MessageMiddlewareQueue, outputWorkerCount)
		for id := 0; id < outputWorkerCount; id++ {

			log.Infof("Declaring output queue %s: %s", strconv.Itoa(id+1), outputQueueName)

			outputQueues[i][id] = middleware.NewMessageMiddlewareQueue(outputQueueName+"_"+strconv.Itoa(id+1), w.channel)
		}
	}

	idx := 0
	// sendersFinCount := 0
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inQueueResponseChan:
			if msg == "EOF" {
				// log.Debugf("EOF received")
				// sendersFinCount += 1
				// if sendersFinCount >= w.config.InputSenders {
				// 	log.Infof("Broadcasting EOF")
				// 	for _, queues := range outputQueues {
				// 		for _, queue := range queues {
				// 			log.Debugf("Broadcasting EOF to queue: ", queue)
				// 			queue.Send([]byte("EOF"))
				// 		}
				// 	}
				// 	return nil
				// }
				log.Infof("Broadcasting EOF")
				for _, queues := range outputQueues {
					for _, queue := range queues {
						log.Debugf("Broadcasting EOF to queue: ", queue)
						queue.Send([]byte("EOF"))
					}
				}
				return nil
			}

			for i, queues := range outputQueues {
				outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
				if err != nil {
					return err
				}

				receiver := (idx + w.config.ID) % outputWorkerCount
				log.Infof("Forwarding message: %s to worker %d", msg, receiver+1)
				queues[receiver].Send([]byte(msg))
			}
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
