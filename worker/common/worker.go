package common

import (
	"errors"
	"strconv"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/aggregator"
	filter "github.com/fiuba-distribuidos-2C2025/tp1/worker/common/filterer"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/grouper"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/joiner"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const MAIN_QUEUE = 0
const SECONDARY_QUEUE = 1

type WorkerConfig struct {
	MiddlewareUrl   string
	InputQueue      []string
	InputSenders    []string
	OutputQueue     []string
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
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue[MAIN_QUEUE]+"_"+strconv.Itoa(w.config.ID), w.channel)
	log.Infof("Input queue declared: %s", w.config.InputQueue[MAIN_QUEUE]+"_"+strconv.Itoa(w.config.ID))
	neededEof, err := strconv.Atoi(w.config.InputSenders[MAIN_QUEUE])
	if err != nil {
		return err
	}

	var secondaryQueueMessages string
	if w.config.WorkerJob == "JOINER_BY_ITEM_ID" {
		secondaryQueueMessages := w.listenToSecondaryQueue()
		if secondaryQueueMessages == "" {
			return nil
		}
	}

	switch w.config.WorkerJob {
	// First Query
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AMOUNT_FILTER":
		log.Info("Starting AMOUNT_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByAmountFilterCallbackWithOutput(inQueueResponseChan, neededEof))
		// Second Query
	case "YEAR_FILTER_ITEMS":
		log.Info("Starting YEAR_FILTER_ITEMS worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterItemsCallbackWithOutput(inQueueResponseChan, neededEof))
	case "GROUPER_BY_YEAR_MONTH":
		log.Info("Starting GROUPER_BY_YEAR_MONTH worker...")
		inQueue.StartConsuming(grouper.CreateByYearMonthGrouperCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AGGREGATOR_BY_PROFIT_QUANTITY":
		log.Info("Starting AGGREGATOR_BY_PROFIT_QUANTITY worker...")
		inQueue.StartConsuming(aggregator.CreateByYearMonthGrouperCallbackWithOutput(inQueueResponseChan, neededEof))

	case "JOINER_BY_ITEM_ID":
		log.Info("Starting JOINER_BY_ITEM_ID worker...")
		inQueue.StartConsuming(joiner.CreateByItemIdJoinerCallbackWithOutput(inQueueResponseChan, neededEof, secondaryQueueMessages))

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
				log.Infof("Forwarding message: %s to worker %d with ", msg, receiver+1)
				queues[receiver].Send([]byte(msg))
			}
			idx += 1
		}
		// TODO: know when input queue is finished!
	}
}

func (w *Worker) listenToSecondaryQueue() string {
	inQueueResponseChan := make(chan string)
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue[SECONDARY_QUEUE]+"_"+strconv.Itoa(w.config.ID), w.channel)
	neededEof, err := strconv.Atoi(w.config.InputSenders[SECONDARY_QUEUE])
	if err != nil {
		return "" // TODO: should return error
	}
	inQueue.StartConsuming(joiner.CreateMenuItemsCallbackWithOutput(inQueueResponseChan, neededEof))

	messages := ""

	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return ""

		case msg := <-inQueueResponseChan:
			if msg == "EOF" {
				return messages
			}
			messages += msg
		}
	}
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
