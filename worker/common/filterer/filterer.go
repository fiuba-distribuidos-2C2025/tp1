package filterer

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// FilterFunc represents a function that filters and potentially transforms a transaction
type FilterFunc func(transaction string) (string, bool)

func resendClientEofs(clientsEofCount map[string]int, neededEof int, outChan chan string, baseDir string, workerID string, messageSentNofificationChan chan string) {
	for clientID, eofCount := range clientsEofCount {
		if eofCount >= neededEof {
			msgID := workerID
			outChan <- clientID + "\n" + msgID + "\nEOF"
			// Here we just block until we are notified that the message was sent
			<-messageSentNofificationChan
			utils.RemoveClientDir(baseDir, clientID)
			delete(clientsEofCount, clientID)
		}
	}
}

// Generic filter callback that handles the common message processing pattern
// All specific filters follow the same structure, only differing in the filter function used
func CreateGenericFilterCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, filterFunc FilterFunc, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	// Load existing clients EOF count in case of worker restart
	clientsEofCount, err := utils.LoadClientsEofCount(baseDir)
	if err != nil {
		log.Errorf("Error loading clients EOF count: %v", err)
		return nil
	}
	resendClientEofs(clientsEofCount, neededEof, outChan, baseDir, workerID, messageSentNotificationChan)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		// Reusable buffer for building output
		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("Received message")
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}

				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]
				msgID := lines[1]

				transactions := lines[2:]
				if transactions[0] == "EOF" {
					// Store EOF on disk as tracking the count in memory is not secure
					// in case of worker restart
					utils.StoreEOF(baseDir, clientID, msgID)
					// Acknowledge message
					msg.Ack(false)
					if _, exists := clientsEofCount[clientID]; !exists {
						clientsEofCount[clientID] = 1
					} else {
						clientsEofCount[clientID]++
					}

					eofCount := clientsEofCount[clientID]
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						// We use the workerID as msgID for the EOF
						// to ensure uniqueness across workers
						msgID := workerID
						outChan <- clientID + "\n" + msgID + "\nEOF"
						<-messageSentNotificationChan
						// clear accumulator memory
						delete(clientsEofCount, clientID)
						utils.RemoveClientDir(baseDir, clientID)
					}
					continue
				}

				outBuilder.Reset()
				for _, transaction := range transactions {
					if filtered, ok := filterFunc(transaction); ok {
						outBuilder.WriteString(filtered + "\n")
					}
				}

				if outBuilder.Len() > 0 {
					// Keep the same msgID in case the worker restarts
					outChan <- clientID + "\n" + msgID + "\n" + outBuilder.String()
					// Here we just block until we are notified that the message was sent
					<-messageSentNotificationChan
					log.Infof("Processed message")
				}
				// Acknowledge message
				msg.Ack(false)
			}
		}
	}
}
