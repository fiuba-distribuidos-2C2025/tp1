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

const EOF_MESSAGE_MAX_LENGTH = 16

type ResponseBuilderConfig struct {
	MiddlewareUrl           string
	WorkerResultsOneCount   int
	WorkerResultsTwoCount   int
	WorkerResultsThreeCount int
	WorkerResultsFourCount  int
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

	clientFinalResult := make(map[string]map[int][]string)
	clientTotalEOFsPerQuery := make(map[string]map[int]int)
	for {
		select {
		case msg := <-outChan:
			lines := strings.Split(msg.Value, "\n")
			clientId := lines[0]
			message := lines[1:]
			log.Infof("Result received from query %d for client %s", msg.ID, clientId)
			if strings.Contains(message[0], "EOF") {
				log.Infof("EOF received from client %s", clientId)

				var expectedEof int
				switch msg.ID {
				case 1:
					expectedEof = m.Config.WorkerResultsOneCount
				case 2:
					expectedEof = m.Config.WorkerResultsTwoCount
				case 3:
					expectedEof = m.Config.WorkerResultsThreeCount
				case 4:
					expectedEof = m.Config.WorkerResultsFourCount
				}

				if _, ok := clientTotalEOFsPerQuery[clientId]; ok {
					if _, ok := clientTotalEOFsPerQuery[clientId][msg.ID]; ok {
						clientTotalEOFsPerQuery[clientId][msg.ID]++
					} else {
						clientTotalEOFsPerQuery[clientId][msg.ID] = 1
					}
				} else {
					clientTotalEOFsPerQuery[clientId] = make(map[int]int)
					clientTotalEOFsPerQuery[clientId][msg.ID] = 1
				}

				totalEOFsInQuery := clientTotalEOFsPerQuery[clientId][msg.ID]
				log.Infof("TOTAL EOFS: %d, EXPECTED: %d", totalEOFsInQuery, expectedEof)
				if totalEOFsInQuery == expectedEof {
					log.Infof("Total EOFs received from client %s, sending query %d result", clientId, msg.ID)
					finalResultsExchange := middleware.NewMessageMiddlewareQueue(fmt.Sprintf("final_results_%s_%d", clientId, msg.ID), channel)
					finalResultsExchange.Send([]byte(strings.Join(clientFinalResult[clientId][msg.ID], "\n")))

					// Clear the accumulated results and EOF count for this query
					clientFinalResult[clientId][msg.ID] = nil
					clientTotalEOFsPerQuery[clientId][msg.ID] = 0
				}
				continue
			}

			if _, ok := clientFinalResult[clientId]; ok {
				if _, ok := clientFinalResult[clientId][msg.ID]; ok {
					// SPLITING AND JOINING, NOT GOOD
					clientFinalResult[clientId][msg.ID] = append(clientFinalResult[clientId][msg.ID], strings.Join(message, "\n"))
				} else {
					clientFinalResult[clientId][msg.ID] = []string{strings.Join(message, "\n")}
				}
			} else {
				clientFinalResult[clientId] = make(map[int][]string)
				clientFinalResult[clientId][msg.ID] = []string{strings.Join(message, "\n")}
			}
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
