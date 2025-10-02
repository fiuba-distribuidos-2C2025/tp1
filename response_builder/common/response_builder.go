package common

import (
	"fmt"
	"net"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type ResponseBuilderConfig struct {
	MiddlewareUrl string
	WorkerCount   int
}

type ResponseBuilder struct {
	Config      ResponseBuilderConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

type ResultMessage struct {
	ID    int
	Value string
}

func NewResponseBuilder(config ResponseBuilderConfig) *ResponseBuilder {
	return &ResponseBuilder{
		Config:      config,
		listener:    nil,
		workerAddrs: nil,
		shutdown:    make(chan struct{}),
	}
}

func (m *ResponseBuilder) Start() error {
	log.Info("Starting response builder")
	rabbit_conn, _ := amqp.Dial(m.Config.MiddlewareUrl)
	channel, _ := rabbit_conn.Channel()
	outChan := make(chan ResultMessage)
	for resultID := 1; resultID <= 4; resultID++ {
		log.Infof("Listening on queue results_%d", resultID)
		doneChan := make(chan error)
		resultsExchange := middleware.NewMessageMiddlewareQueue(fmt.Sprintf("results_%d_1", resultID), channel)
		resultsExchange.StartConsuming(createResultsCallback(outChan, doneChan, resultID))
	}

	final_result := make(map[int][]string)
	totalEOFsPerQuery := make(map[int]int)
	for {
		select {
		case msg := <-outChan:
			if msg.Value == "EOF" {
				log.Info("EOF received")
				totalEOFsPerQuery[msg.ID]++
				log.Infof("TOTAL EOFS: %d, EXPECTED: %d", totalEOFsPerQuery[msg.ID], m.Config.WorkerCount)
				if totalEOFsPerQuery[msg.ID] == m.Config.WorkerCount {
					finalResultsExchange := middleware.NewMessageMiddlewareQueue(fmt.Sprintf("final_results_%d", msg.ID), channel)
					finalResultsExchange.Send([]byte(strings.Join(final_result[msg.ID], "\n")))
					log.Info("Total EOFS received, sending query ", msg.ID, " result")
				}
				continue
			}

			log.Info("Result received query ", msg.ID)
			final_result[msg.ID] = append(final_result[msg.ID], msg.Value)
		}
	}
}

// createResultsCallback creates a callback function for consuming results
func createResultsCallback(resultChan chan ResultMessage, doneChan chan error, resultID int) func(middleware.ConsumeChannel, chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Info("Results consumer started")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Info("Consume channel closed")
					doneChan <- fmt.Errorf("channel closed unexpectedly")
					return
				}

				log.Infof("Message received - Length: %d, Body: %s", len(msg.Body), string(msg.Body))

				if err := msg.Ack(false); err != nil {
					log.Errorf("Failed to acknowledge message: %v", err)
					doneChan <- fmt.Errorf("failed to ack message: %w", err)
					return
				}

				resultChan <- ResultMessage{
					Value: string(msg.Body),
					ID:    resultID,
				}

			case err := <-done:
				log.Errorf("Consumer error: %v", err)
				doneChan <- err
			}
		}
	}
}
