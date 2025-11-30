package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Resend messages in case of worker restart
func resendClientMessages(clientMessages map[string]map[string]string, clientEofs map[string]map[string]string, neededEof int, outChan chan string) {
	// Check which clients have reached the EOF threshold
	for clientID, eofs := range clientEofs {
		eofCount := len(eofs)
		if eofCount >= neededEof {
			// Send all messages for this client
			clientMessagesData := processClientMessages(clientMessages, clientID)

			outChan <- clientID + "\n" + clientMessagesData
			// remove the client entry from the map
			delete(clientEofs, clientID)
			delete(clientMessages, clientID)
			// Note: We do not remove the stored messages/EOFs from disk here, as they will be removed when the primary queue
			// finishes processing the client.
		}
	}
}

func messageAlreadyReceived(clientMessages map[string]map[string]string, clientEofs map[string]map[string]string, clientID string, msgID string) bool {
	// Check if message already received
	if _, exists := clientMessages[clientID]; !exists {
		clientMessages[clientID] = make(map[string]string)
	}
	if _, exists := clientMessages[clientID][msgID]; exists {
		return true
	}
	// Check if EOF already received
	if _, exists := clientEofs[clientID]; !exists {
		clientEofs[clientID] = make(map[string]string)
	}
	if _, exists := clientEofs[clientID][msgID]; exists {
		// Acknowledge duplicate EOF
		return true
	}
	return false
}

func processClientMessages(clientMessages map[string]map[string]string, clientID string) string {
	messages := clientMessages[clientID]
	var builder strings.Builder
	for _, items := range messages {
		builder.WriteString(items)
		builder.WriteString("\n")
	}
	return strings.TrimSuffix(builder.String(), "\n")
}

func processPendingMessagesForClient(clientID string, secondaryQueueItems map[string]map[string]string, clientEofs map[string]map[string]string, outChan chan string, messageSentNotificationChan chan string, baseDir string, workerID string, neededEof int, joinCallback func(transaction string, menuItemsData map[string]string) (string, bool)) {
	// Load client messages from disk
	clientMessages, err := utils.LoadClientMessagesWithChecksums(baseDir, clientID)
	if err != nil {
		log.Errorf("Error loading messages for client %s: %v", clientID, err)
		return
	}
	// Send all messages for this client
	var outBuilder strings.Builder

	for msgID, msg := range clientMessages {
		lines := strings.Split(msg, "\n")
		items := lines[2:]
		for _, transaction := range items {
			if concatenated, ok := joinCallback(transaction, secondaryQueueItems[clientID]); ok {
				outBuilder.WriteString(concatenated)
				outBuilder.WriteByte('\n')
			}
		}

		if outBuilder.Len() > 0 {
			// Reuse the same msgID as it is already unique
			// and persistent across worker restarts
			outChan <- clientID + "\n" + msgID + "\n" + strings.TrimSuffix(outBuilder.String(), "\n")
			// Here we just block until we are notified that the message was sent
			<-messageSentNotificationChan
			log.Infof("Processed message")
		}
		outBuilder.Reset()
	}

	eofs := clientEofs[clientID]
	eofCount := len(eofs)
	if eofCount >= neededEof {
		// Use the workerID as msgID for the EOF
		// to ensure uniqueness across workers and restarts
		outChan <- clientID + "\n" + workerID + "\nEOF"
		// Here we just block until we are notified that the message was sent
		<-messageSentNotificationChan
		// clear accumulator memory
		delete(clientEofs, clientID)
		delete(secondaryQueueItems, clientID)
		utils.RemoveClientDir(baseDir, clientID)
		utils.RemoveClientDir(baseDir+"/secondary", clientID)
	}
}

func CreateSecondQueueCallbackWithOutput(outChan chan string, neededEof int, baseDir string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		// Load existing clients EOF in case of worker restart
		clientEofs, err := utils.LoadClientsEofs(baseDir)
		if err != nil {
			log.Errorf("Error loading clients EOF: %v", err)
			return
		}

		// Load existing clients messages in case of worker restart
		clientMessages, err := utils.LoadAllClientsMessagesWithChecksums(baseDir)
		if err != nil {
			log.Errorf("Error loading client messages: %v", err)
			return
		}

		// Resend messages for clients that have already reached the EOF threshold
		resendClientMessages(clientMessages, clientEofs, neededEof, outChan)

		log.Infof("Waiting for secondary queue messages...")
		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("SECONDARY QUEUE MESSAGE RECEIVED")
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.SplitN(payload, "\n", 3)

				// Separate header and the rest
				clientID := lines[0]
				msgID := lines[1]
				items := lines[2]

				// Check if message already received
				if messageAlreadyReceived(clientMessages, clientEofs, clientID, msgID) {
					// Acknowledge message
					msg.Ack(false)
					continue
				}

				// Store message or EOF on disk
				if lines[2] == "EOF" {
					utils.StoreEOF(baseDir, clientID, msgID)
				} else {
					utils.StoreMessageWithChecksum(baseDir, clientID, msgID, items)
				}

				// Acknowledge message
				msg.Ack(false)

				if items == "EOF" {
					// Store EOF in memory
					clientEofs[clientID][msgID] = ""

					eofCount := len(clientEofs[clientID])
					if eofCount >= neededEof {
						// Send all messages for this client
						clientMessagesData := processClientMessages(clientMessages, clientID)

						outChan <- clientID + "\n" + clientMessagesData
						// remove the client entry from the map
						delete(clientEofs, clientID)
						delete(clientMessages, clientID)
						// Note: We do not remove the stored messages/EOFs from disk here, as they will be removed when the primary queue
						// finishes processing the client.
					}
					continue
				}

				if items != "" {
					// Store message in memory
					clientMessages[clientID][msgID] = items
				}
			}
		}
	}
}
