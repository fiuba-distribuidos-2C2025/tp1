package common

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
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
	IsTest          bool
	BaseDir         string
}

type Worker struct {
	config       WorkerConfig
	shutdown     chan struct{}
	conn         *amqp.Connection
	channel      *amqp.Channel
	queueFactory middleware.QueueFactory
}

func NewWorker(config WorkerConfig, factory middleware.QueueFactory) (*Worker, error) {
	if !config.IsTest {
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
		factory.SetChannel(ch)
		return &Worker{
			config:       config,
			shutdown:     make(chan struct{}, 1),
			conn:         conn,
			channel:      ch,
			queueFactory: factory,
		}, nil
	} else {
		return &Worker{
			config:       config,
			shutdown:     make(chan struct{}, 1),
			queueFactory: factory,
		}, nil
	}
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
	if !w.config.IsTest {
		defer func() {
			_ = w.channel.Close()
			_ = w.conn.Close()
		}()
	}

	// var secondaryQueueMessages string
	secondaryQueueMessagesChan := make(chan string)
	if hasSecondaryQueue(w.config.WorkerJob) {
		go func() {
			log.Debugf("Worker has secondary %s, reading it", w.config.InputQueue[SECONDARY_QUEUE]+"_"+strconv.Itoa(w.config.ID))
			w.listenToSecondaryQueue(secondaryQueueMessagesChan)
		}()
	}

	inQueueResponseChan := make(chan string)
	mesageSentNotificationChan := make(chan string)
	w.listenToPrimaryQueue(inQueueResponseChan, mesageSentNotificationChan, secondaryQueueMessagesChan)

	outputQueues, err := w.setupOutputQueues()
	if err != nil {
		return err
	}

	if shouldBroadcast(w.config.WorkerJob) {
		log.Infof("Worker job %s requires broadcasting", w.config.WorkerJob)
		return w.broadcastMessages(inQueueResponseChan, outputQueues)
	}

	if shouldDistributeBetweenAggregators(w.config.WorkerJob) {
		log.Infof("Worker job %s requires distribution between aggregators", w.config.WorkerJob)
		return w.distributeBetweenAggregators(inQueueResponseChan, outputQueues)
	}

	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inQueueResponseChan:

			lines := strings.SplitN(msg, "\n", 3)

			if lines[2] == "EOF" {
				log.Infof("Broadcasting EOF for client %s", lines[0])
				for _, queues := range outputQueues {
					for _, queue := range queues {
						log.Debugf("Broadcasting EOF to queue: ", queue)
						queue.Send([]byte(msg))
					}
				}
				if shouldNotify(w.config.WorkerJob) {
					mesageSentNotificationChan <- "sent"
				}
				continue
			}

			msgID := lines[1]

			for i, queues := range outputQueues {
				outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
				if err != nil {
					return fmt.Errorf("invalid OutputReceivers[%d]=%q: %w", i, w.config.OutputReceivers[i], err)
				}
				// In case of worker restarts, ensure same distribution
				workerIdx := chooseWorkerIdx(msgID, outputWorkerCount)
				msg_truncated := msg
				if len(msg_truncated) > 200 {
					msg_truncated = msg_truncated[:200] + "..."
				}
				log.Infof("Forwarding message:\n%s\nto worker %d", msg_truncated, workerIdx+1)

				queues[workerIdx].Send([]byte(msg))
			}
			if shouldNotify(w.config.WorkerJob) {
				mesageSentNotificationChan <- "sent"
			}
		}
	}
}

func shouldNotify(workerJob string) bool {
	switch workerJob {
	case "YEAR_FILTER":
		return true
	case "HOUR_FILTER":
		return true
	case "AMOUNT_FILTER":
		return true
	case "YEAR_FILTER_ITEMS":
		return true
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

func (w *Worker) setupOutputQueues() ([][]middleware.MessageMiddleware, error) {
	outputQueues := make([][]middleware.MessageMiddleware, len(w.config.OutputReceivers))

	for i := 0; i < len(w.config.OutputReceivers); i++ {
		outputQueueName := w.config.OutputQueue[i]
		outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
		if err != nil {
			return nil, err
		}

		outputQueues[i] = make([]middleware.MessageMiddleware, outputWorkerCount)
		for j := 0; j < outputWorkerCount; j++ {
			queueName := outputQueueName + "_" + strconv.Itoa(j+1)
			log.Infof("Declaring output queue %d: %s", j+1, queueName)

			// Factory returns the interface - works for both real and mock!
			outputQueues[i][j] = w.queueFactory.CreateQueue(queueName)
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

func (w *Worker) listenToSecondaryQueue(secondaryQueueMessagesChan chan string) {
	inQueueResponseChan := make(chan string)
	inQueue := w.queueFactory.CreateQueue(w.config.InputQueue[SECONDARY_QUEUE] + "_" + strconv.Itoa(w.config.ID))
	neededEof, err := strconv.Atoi(w.config.InputSenders[SECONDARY_QUEUE])
	if err != nil {
		return // TODO: should return error
	}
	// All joiners use the same callback.
	inQueue.StartConsuming(joiner.CreateSecondQueueCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir+"/secondary"))

	clientMessages := make(map[string]string)
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping secondary queue message listening...")
			return

		case msg := <-inQueueResponseChan:
			lines := strings.SplitN(msg, "\n", 2)
			clientId := lines[0]

			if strings.Contains(msg, "EOF") {
				secondaryQueueMessagesChan <- clientId + "\n" + clientMessages[clientId]
				// Clear stored messages for client
				delete(clientMessages, clientId)
				continue
			}

			items := lines[1]
			if _, ok := clientMessages[clientId]; !ok {
				clientMessages[clientId] = items
			} else {
				clientMessages[clientId] += items
			}
		}
	}
}

func (w *Worker) listenToPrimaryQueue(inQueueResponseChan chan string, messageSentNotificationChan chan string, secondaryQueueMessagesChan chan string) error {
	log.Infof("Declaring input queue: %s", w.config.InputQueue[MAIN_QUEUE]+"_"+strconv.Itoa(w.config.ID))
	inQueue := w.queueFactory.CreateQueue(w.config.InputQueue[MAIN_QUEUE] + "_" + strconv.Itoa(w.config.ID))
	neededEof, err := strconv.Atoi(w.config.InputSenders[MAIN_QUEUE])
	if err != nil {
		log.Errorf("Failed to parse needed EOF count: %v", err)
		return err
	}

	log.Infof("Setting up worker job: %s", w.config.WorkerJob)
	switch w.config.WorkerJob {
	// ==============================================================================
	// First Query
	// ==============================================================================
	case "YEAR_FILTER":
		log.Info("Starting YEAR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	case "HOUR_FILTER":
		log.Info("Starting HOUR_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByHourFilterCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	case "AMOUNT_FILTER":
		log.Info("Starting AMOUNT_FILTER worker...")
		inQueue.StartConsuming(filter.CreateByAmountFilterCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	// ==============================================================================
	// Second Query
	// ==============================================================================
	case "YEAR_FILTER_ITEMS":
		log.Info("Starting YEAR_FILTER_ITEMS worker...")
		inQueue.StartConsuming(filter.CreateByYearFilterItemsCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	case "GROUPER_BY_YEAR_MONTH":
		log.Info("Starting GROUPER_BY_YEAR_MONTH worker...")
		inQueue.StartConsuming(grouper.CreateGrouperCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, grouper.ThresholdReachedHandleByYearMonth))
	case "AGGREGATOR_BY_PROFIT_QUANTITY":
		log.Info("Starting AGGREGATOR_BY_PROFIT_QUANTITY worker...")
		inQueue.StartConsuming(aggregator.CreateAggregatorCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, aggregator.ThresholdReachedHandleProfitQuantity))
	case "JOINER_BY_ITEM_ID":
		log.Info("Starting JOINER_BY_ITEM_ID worker...")
		inQueue.StartConsuming(joiner.CreateByItemIdJoinerCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, secondaryQueueMessagesChan, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	// ==============================================================================
	// Third Query
	// ==============================================================================
	case "GROUPER_BY_SEMESTER":
		log.Info("Starting GROUPER_BY_SEMESTER worker...")
		inQueue.StartConsuming(grouper.CreateGrouperCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, grouper.ThresholdReachedHandleSemester))
	case "AGGREGATOR_SEMESTER":
		log.Info("Starting AGGREGATOR_SEMESTER worker...")
		inQueue.StartConsuming(aggregator.CreateAggregatorCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, aggregator.ThresholdReachedHandleSemester))
	case "JOINER_BY_STORE_ID":
		log.Info("Starting JOINER_BY_STORE_ID worker...")
		inQueue.StartConsuming(joiner.CreateByStoreIdJoinerCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, secondaryQueueMessagesChan, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	// ==============================================================================
	// Fourth Query
	// ==============================================================================
	case "GROUPER_BY_STORE_USER":
		log.Info("Starting GROUPER_BY_STORE_USER worker...")
		inQueue.StartConsuming(grouper.CreateGrouperCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, grouper.ThresholdReachedHandleByStoreUser))
	case "AGGREGATOR_BY_STORE_USER":
		log.Info("Starting AGGREGATOR_BY_STORE_USER worker...")
		inQueue.StartConsuming(aggregator.CreateAggregatorCallbackWithOutput(inQueueResponseChan, neededEof, w.config.BaseDir, aggregator.ThresholdReachedHandleStoreUser))
	case "JOINER_BY_USER_ID":
		log.Info("Starting JOINER_BY_USER_ID worker...")
		inQueue.StartConsuming(joiner.CreateByUserIdJoinerCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, secondaryQueueMessagesChan, w.config.BaseDir, strconv.Itoa(w.config.ID)))
	case "JOINER_BY_USER_STORE":
		log.Info("starting JOINER_BY_USER_STORE worker...")
		inQueue.StartConsuming(joiner.CreateByUserStoreIdJoinerCallbackWithOutput(inQueueResponseChan, messageSentNotificationChan, neededEof, secondaryQueueMessagesChan, w.config.BaseDir, strconv.Itoa(w.config.ID)))

	default:
		log.Error("Unknown worker job")
		return errors.New("unknown worker job")
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

func shouldDistributeBetweenAggregators(workerJob string) bool {
	switch workerJob {
	case "GROUPER_BY_YEAR_MONTH":
		return true
	case "GROUPER_BY_SEMESTER":
		return true
	case "GROUPER_BY_STORE_USER":
		return true
	default:
		return false
	}
}

func (w *Worker) broadcastMessages(inChan chan string, outputQueues [][]middleware.MessageMiddleware) error {
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inChan:
			log.Infof("Broadcasting message:\n%s\n", msg)
			for _, queues := range outputQueues {
				for _, queue := range queues {
					queue.Send([]byte(msg))
				}
			}
		}
	}
}

func hashKey64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func chooseWorkerIdx(key string, workerCount int) int {
	return int(hashKey64(key) % uint64(workerCount))
}

func (w *Worker) distributeBetweenAggregators(inChan chan string, outputQueues [][]middleware.MessageMiddleware) error {
	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil

		case msg := <-inChan:
			lines := strings.SplitN(msg, "\n", 2)
			// generate a random message ID
			// this is a workaround until we refactor the grouper to
			// properly handle message IDs
			msgID := fmt.Sprintf("%d", rand.Int63())

			if lines[1] == "EOF" {
				log.Infof("Broadcasting EOF")
				for _, queues := range outputQueues {
					for _, queue := range queues {
						log.Debugf("Broadcasting EOF to queue: ", queue)

						msg = lines[0] + "\n" + msgID + "\n" + lines[1]
						queue.Send([]byte(msg))

					}
				}
				continue
			}
			lines = strings.SplitN(msg, "\n", 3)
			clientID := lines[0]
			key := lines[1]
			msg = clientID + "\n" + msgID + "\n" + lines[2]

			for i, queues := range outputQueues {
				outputWorkerCount, err := strconv.Atoi(w.config.OutputReceivers[i])
				if err != nil {
					return fmt.Errorf("invalid OutputReceivers[%d]=%q: %w", i, w.config.OutputReceivers[i], err)
				}
				aggregatorIdx := chooseWorkerIdx(key, outputWorkerCount)

				msg_truncated := msg
				if len(msg_truncated) > 200 {
					msg_truncated = msg_truncated[:200] + "..."
				}
				log.Infof("Forwarding message:\n%s\nto worker %d", msg_truncated, aggregatorIdx+1)

				queues[aggregatorIdx].Send([]byte(msg))

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
