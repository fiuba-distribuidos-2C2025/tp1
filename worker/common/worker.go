package common

import (
	"errors"
	"strconv"
	"strings"
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
const EOF_MESSAGE_MAX_LENGTH = 16

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

	var secondaryQueueMessages string
	if hasSecondaryQueue(w.config.WorkerJob) {
		log.Debugf("Worker has secondary %s, reading it", w.config.InputQueue[SECONDARY_QUEUE]+"_"+strconv.Itoa(w.config.ID))
		secondaryQueueMessages = w.listenToSecondaryQueue()
		if secondaryQueueMessages == "" {
			return nil
		}
		log.Debugf("Finished reading from secondary queue")
	}

	inQueueResponseChan := make(chan string)
	w.listenToPrimaryQueue(inQueueResponseChan, secondaryQueueMessages)

	outputQueues, err := w.setupOutputQueues()
	if err != nil {
		return err
	}

	if shouldBroadcast(w.config.WorkerJob) {
		log.Infof("Worker job %s requires broadcasting", w.config.WorkerJob)
		return w.broadcastMessages(inQueueResponseChan, outputQueues)
	}

	idx := 0
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inQueueResponseChan:
			// Not that good, we are assuming that short messages are
			// EOF, this doesn't scale with clients of id really high
			// (probably will never happen)
			if len(msg) < EOF_MESSAGE_MAX_LENGTH && strings.Contains(msg, "EOF") {
				log.Infof("Broadcasting EOF")
				for _, queues := range outputQueues {
					for _, queue := range queues {
						log.Debugf("Broadcasting EOF to queue: ", queue)
						queue.Send([]byte(msg))
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
				msg_truncated := msg
				if len(msg_truncated) > 200 {
					msg_truncated = msg_truncated[:200] + "..."
				}
				log.Infof("Forwarding message:\n%s\nto worker %d", msg_truncated, receiver+1)
				queues[receiver].Send([]byte(msg))
			}
			idx += 1
		}
	}
}

func (w *Worker) setupOutputQueues() ([][]*middleware.MessageMiddlewareQueue, error) {
	outputQueues := make([][]*middleware.MessageMiddlewareQueue, len(w.config.OutputReceivers))
	for i := 0; i < len(w.config.OutputReceivers); i++ {
		outputQueueName := w.config.OutputQueue[i]
		outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
		if err != nil {
			return nil, err
		}

		outputQueues[i] = make([]*middleware.MessageMiddlewareQueue, outputWorkerCount)
		for j := 0; j < outputWorkerCount; j++ {
			log.Infof("Declaring output queue %s: %s", strconv.Itoa(j+1), outputQueueName)
			outputQueues[i][j] = middleware.NewMessageMiddlewareQueue(outputQueueName+"_"+strconv.Itoa(j+1), w.channel)
		}
	}
	return outputQueues, nil
}

func hasSecondaryQueue(workerJob string) bool {
	switch workerJob {
	case "JOINER_BY_ITEM_ID":
		return true
	case "JOINER_BY_STORE_ID":
		return true
	case "JOINER_BY_USER_ID":
		return true
	case "JOINER_BY_USER_STORE":
		return true
	default:
		return false
	}
}

func (w *Worker) listenToSecondaryQueue() string {
	inQueueResponseChan := make(chan string)
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue[SECONDARY_QUEUE]+"_"+strconv.Itoa(w.config.ID), w.channel)
	neededEof, err := strconv.Atoi(w.config.InputSenders[SECONDARY_QUEUE])
	if err != nil {
		return "" // TODO: should return error
	}
	// All joiners use the same callback.
	inQueue.StartConsuming(joiner.CreateSecondQueueCallbackWithOutput(inQueueResponseChan, neededEof))

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

func (w *Worker) listenToPrimaryQueue(inQueueResponseChan chan string, secondaryQueueMessages string) error {
	inQueue := middleware.NewMessageMiddlewareQueue(w.config.InputQueue[MAIN_QUEUE]+"_"+strconv.Itoa(w.config.ID), w.channel)
	neededEof, err := strconv.Atoi(w.config.InputSenders[MAIN_QUEUE])
	if err != nil {
		return err
	}

	switch w.config.WorkerJob {
	// ==============================================================================
	// First Query
	// ==============================================================================
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AMOUNT_FILTER":
		log.Info("Starting AMOUNT_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByAmountFilterCallbackWithOutput(inQueueResponseChan, neededEof))
	// ==============================================================================
	// Second Query
	// ==============================================================================
	case "YEAR_FILTER_ITEMS":
		log.Info("Starting YEAR_FILTER_ITEMS worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterItemsCallbackWithOutput(inQueueResponseChan, neededEof))
	case "GROUPER_BY_YEAR_MONTH":
		log.Info("Starting GROUPER_BY_YEAR_MONTH worker...")
		inQueue.StartConsuming(grouper.CreateByYearMonthGrouperCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AGGREGATOR_BY_PROFIT_QUANTITY":
		log.Info("Starting AGGREGATOR_BY_PROFIT_QUANTITY worker...")
		inQueue.StartConsuming(aggregator.CreateByQuantityProfitAggregatorCallbackWithOutput(inQueueResponseChan, neededEof))
	case "JOINER_BY_ITEM_ID":
		log.Info("Starting JOINER_BY_ITEM_ID worker...")
		inQueue.StartConsuming(joiner.CreateByItemIdJoinerCallbackWithOutput(inQueueResponseChan, neededEof, secondaryQueueMessages))
	// ==============================================================================
	// Third Query
	// ==============================================================================
	case "GROUPER_BY_SEMESTER":
		log.Info("Starting GROUPER_BY_SEMESTER worker...")
		inQueue.StartConsuming(grouper.CreateBySemesterGrouperCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AGGREGATOR_SEMESTER":
		log.Info("Starting AGGREGATOR_SEMESTER worker...")
		inQueue.StartConsuming(aggregator.CreateBySemesterAggregatorCallbackWithOutput(inQueueResponseChan, neededEof))
	case "JOINER_BY_STORE_ID":
		log.Info("Starting JOINER_BY_STORE_ID worker...")
		inQueue.StartConsuming(joiner.CreateByStoreIdJoinerCallbackWithOutput(inQueueResponseChan, neededEof, secondaryQueueMessages))
	// ==============================================================================
	// Fourth Query
	// ==============================================================================
	case "GROUPER_BY_STORE_USER":
		log.Info("Starting GROUPER_BY_STORE_USER worker...")
		inQueue.StartConsuming(grouper.CreateByStoreUserGrouperCallbackWithOutput(inQueueResponseChan, neededEof))
	case "AGGREGATOR_BY_STORE_USER":
		log.Info("Starting AGGREGATOR_BY_STORE_USER worker...")
		inQueue.StartConsuming(aggregator.CreateByStoreUserAggregatorCallbackWithOutput(inQueueResponseChan, neededEof))
	case "JOINER_BY_USER_ID":
		log.Info("Starting JOINER_BY_USER_ID worker...")
		inQueue.StartConsuming(joiner.CreateByUserIdJoinerCallbackWithOutput(inQueueResponseChan, neededEof, secondaryQueueMessages))
	case "JOINER_BY_USER_STORE":
		log.Info("Starting JOINER_BY_USER_STORE worker...")
		inQueue.StartConsuming(joiner.CreateByUserStoreIdJoinerCallbackWithOutput(inQueueResponseChan, neededEof, secondaryQueueMessages))

	default:
		log.Error("Unknown worker job")
		return errors.New("Unknown worker job")
	}

	log.Infof("Input queue declared: %s", w.config.InputQueue[MAIN_QUEUE]+"_"+strconv.Itoa(w.config.ID))
	return nil
}

func shouldBroadcast(workerJob string) bool {
	switch workerJob {
	case "AGGREGATOR_BY_STORE_USER":
		return true
	default:
		return false
	}
}

func (w *Worker) broadcastMessages(inChan chan string, outputQueues [][]*middleware.MessageMiddlewareQueue) error {
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inChan:
			if msg == "EOF" {
				log.Infof("Broadcasting EOF")
				for _, queues := range outputQueues {
					for _, queue := range queues {
						log.Debugf("Broadcasting EOF to queue: ", queue)
						queue.Send([]byte("EOF"))
					}
				}
				return nil
			}
			for _, queues := range outputQueues {
				for _, queue := range queues {
					log.Infof("Forwarding message:\n%s\nto queue %d with ", msg, queue)
					queue.Send([]byte(msg))
				}
			}
		}
	}
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)

	log.Info("Worker shut down complete.")
}
