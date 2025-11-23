package joiner

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Resend stored messages in case of worker restart
func resendClientMessages(baseDir string, outChan chan string) error {
	clients, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages yet, nothing to do.
			return nil
		}
		return err
	}

	for _, c := range clients {
		if !c.IsDir() {
			continue
		}

		clientID := c.Name()
		messagesDir := filepath.Join(baseDir, clientID, "messages")
		entries, err := os.ReadDir(messagesDir)
		if err != nil {
			if os.IsNotExist(err) {
				// No messages, forward EOF and clean up
				msg := clientID + "\nEOF"
				outChan <- msg
				return utils.RemoveClientDir(baseDir, clientID)
			}
			return err
		}

		for _, e := range entries {
			if e.IsDir() {
				continue // skip nested directories
			}

			filePath := filepath.Join(messagesDir, e.Name())

			data, err := os.ReadFile(filePath)
			payload := strings.TrimSpace(string(data))
			lines := strings.SplitN(payload, "\n", 3)

			// Separate header and the rest
			// clientID := lines[0]
			// msgID := lines[1]
			items := lines[2]
			if err != nil {
				log.Infof("failed to read file %s: %v", filePath, err)
				continue
			}
			if items != "" {
				log.Info("RESENDING THROUGH SECONDARY CHANNEL\n%s", items)
				outChan <- payload
			}

		}
	}
	return nil
}

func ResendClientEofs(clientsEofCount map[string]int, neededEof int, outChan chan string) {
	for clientID, eofCount := range clientsEofCount {
		if eofCount >= neededEof {
			outChan <- clientID + "\nEOF"
			// remove the client entry from the map
			delete(clientsEofCount, clientID)
		}
	}
}

func CreateSecondQueueCallbackWithOutput(outChan chan string, neededEof int, baseDir string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	// Load existing clients EOF count in case of worker restart
	clientsEofCount, err := utils.LoadClientsEofCount(baseDir)
	if err != nil {
		log.Errorf("Error loading clients EOF count: %v", err)
		return nil
	}
	resendClientMessages(baseDir, outChan)
	ResendClientEofs(clientsEofCount, neededEof, outChan)

	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
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

				// Store message or EOF on disk
				if lines[2] == "EOF" {
					utils.StoreEOF(baseDir, clientID, msgID)
				} else {
					utils.StoreMessage(baseDir, clientID, msgID, payload)
				}

				// Acknowledge message
				msg.Ack(false)

				if items == "EOF" {
					if _, exists := clientsEofCount[clientID]; !exists {
						clientsEofCount[clientID] = 1
					} else {
						clientsEofCount[clientID]++
					}

					eofCount := clientsEofCount[clientID]
					if eofCount >= neededEof {
						outChan <- payload
						// remove the client entry from the map
						delete(clientsEofCount, clientID)
						continue
					}
				}

				if items != "" {
					log.Info("SENDING THROUGH SECONDARY CHANNEL\n%s", items)
					outChan <- payload
				}
			}
		}
	}
}
