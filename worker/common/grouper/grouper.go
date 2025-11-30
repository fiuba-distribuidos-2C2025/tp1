package grouper

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func SendBatches(outChan chan string, messageSentNotificationChan chan string, clientID string, workerID string, batches []string, batchesKeys []string) {
	for i, batch := range batches {
		if batch != "" {
			// This ensures deterministic message IDs per batch
			msgID := workerID + "-" + strconv.Itoa(i)
			outChan <- clientID + "\n" + batchesKeys[i] + "\n" + msgID + "\n" + batch
			// Here we just block until we are notified that the message was sent
			<-messageSentNotificationChan
		}
	}
}

func CreateGrouperCallbackWithOutput(outChan chan string, neededEof int, baseDir string, workerID string, messageSentNotificationChan chan string, thresholdReachedHandle func(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		// Check existing EOF thresholds before starting to consume messages.
		// This ensures that if the worker restarts, it sends the aggregated
		// results for clients that have already sent all their data.
		err := utils.CheckAllClientsEOFThresholds(outChan, baseDir, neededEof, workerID, messageSentNotificationChan, thresholdReachedHandle)
		if err != nil {
			log.Errorf("Error checking existing EOF thresholds: %v", err)
		}
		log.Infof("Waiting for messages...")

		for {
			select {
			case <-done:
				log.Info("Shutdown signal received, stopping worker...")
				return

			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}

				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.SplitN(payload, "\n", 3)

				// Separate header and the rest
				clientID := lines[0]
				msgId := lines[1]

				// Store message or EOF on disk
				if lines[2] == "EOF" {
					utils.StoreEOF(baseDir, clientID, msgId)
				} else {
					utils.StoreMessage(baseDir, clientID, msgId, lines[2])
				}

				// Acknowledge message
				msg.Ack(false)

				// Check if threshold reached for this client
				if lines[2] == "EOF" {
					thresholdReached, err := utils.ThresholdReached(baseDir, clientID, neededEof)
					if err != nil {
						log.Errorf("Error checking threshold for client %s: %v", clientID, err)
						return
					}
					if thresholdReached {
						err := thresholdReachedHandle(outChan, messageSentNotificationChan, baseDir, clientID, workerID)
						if err != nil {
							log.Errorf("Error handling threshold reached for client %s: %v", clientID, err)
							return
						}
					}
				}

			}
		}
	}
}
