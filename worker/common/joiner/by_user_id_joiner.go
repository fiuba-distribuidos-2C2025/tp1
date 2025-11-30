package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

func concatTop3WithBirthdates(user string, neededUsers map[string]string) (string, bool) {
	// user_id,gender,birthdate,registered_at
	// 2018392,male,1992-12-27,2025-06-01 09:56:51
	fields := strings.Split(user, ",")
	if len(fields) < 4 {
		log.Errorf("Invalid user format: %s", user)
		return "", false
	}
	userId := fields[0] + ".0" // TODO: datasets show userId in different formats
	birthdate := fields[2]
	var sb strings.Builder
	if _, needed := neededUsers[userId]; needed {
		top3line := neededUsers[userId]
		elements := strings.Split(top3line, ",")
		if len(elements) < 3 {
			return "", false
		}
		// storeId,userId,PurchaseQty:
		// 1,1,10
		storeId := elements[0]
		// userId := elements[1]
		purchaseQty := elements[2]
		sb.WriteString(storeId)
		sb.WriteByte(',')
		sb.WriteString(birthdate)
		sb.WriteByte(',')
		sb.WriteString(purchaseQty)
	} else {
		return "", false
	}
	return sb.String(), true
}

// example store ids data input
// storeId,userId,PurchaseQty:
// 1,1,10
func processNeededUsers(top3Lines []string) map[string]string {
	// Preprocess stores data into a map for quick lookup
	neededUsers := make(map[string]string)
	for _, line := range top3Lines {
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			log.Errorf("Invalid top 3 data format: %s", line)
			continue
		}
		userId := fields[1]
		neededUsers[userId] = line
	}
	return neededUsers
}

func CreateByUserIdJoinerCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, top3PerStoreRowsChan chan string, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		// Load existing clients EOF count in case of worker restart
		clientEofs, err := utils.LoadClientsEofs(baseDir)
		if err != nil {
			log.Errorf("Error loading clients EOF count: %v", err)
			return
		}
		neededUsers := make(map[string]map[string]string)
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case secondaryQueueNewMessage, ok := <-top3PerStoreRowsChan:
				if !ok {
					log.Errorf("top3PerStoreRowsChan closed with error %v", err)
					return
				}

				log.Debugf("Received message from secondary queue being inside primary queue:\n%s", secondaryQueueNewMessage)
				payload := strings.TrimSpace(secondaryQueueNewMessage)
				rows := strings.Split(payload, "\n")
				if len(rows) < 2 {
					log.Errorf("Invalid secondary queue payload (need header + at least one row): %q", payload)
					continue
				}
				secondaryQueueClientID := rows[0]
				clientTop3PerStoreArray := rows[1:]
				clientNeededUsers := processNeededUsers(clientTop3PerStoreArray)
				neededUsers[secondaryQueueClientID] = clientNeededUsers
				processPendingMessagesForClient(secondaryQueueClientID, neededUsers, clientEofs, outChan, messageSentNotificationChan, baseDir, workerID, neededEof, concatTop3WithBirthdates)
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]
				msgID := lines[1]
				items := lines[2:]

				// Create accumulator for client
				if _, exists := neededUsers[clientID]; !exists {
					neededUsers[clientID] = make(map[string]string)
				}

				// Ensure we have the needed users for this client.
				// This will block until the expected clientID arrives.
				// TODO: avoid blocking other clients while waiting for one
				if len(neededUsers[clientID]) == 0 {
					log.Infof("Needed users not yet available for client %s, storing message...", clientID)
					if items[0] == "EOF" {
						utils.StoreEOF(baseDir, clientID, msgID)
						if _, exists := clientEofs[clientID]; !exists {
							clientEofs[clientID] = make(map[string]string)
						}
						clientEofs[clientID][msgID] = ""
					} else {
						utils.StoreMessageWithChecksum(baseDir, clientID, msgID, payload)
					}
					// Acknowledge message
					msg.Ack(false)
					continue
				}

				if items[0] == "EOF" {
					// Store EOF on disk as tracking the count in memory is not secure
					// in case of worker restart
					utils.StoreEOF(baseDir, clientID, msgID)
					// Acknowledge message
					msg.Ack(false)
					if _, exists := clientEofs[clientID]; !exists {
						clientEofs[clientID] = make(map[string]string)
					}
					clientEofs[clientID][msgID] = ""

					eofs := clientEofs[clientID]
					eofCount := len(eofs)
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						// Use the workerID as msgID for the EOF
						// to ensure uniqueness across workers and restarts
						outChan <- clientID + "\n" + workerID + "\nEOF"
						// Here we just block until we are notified that the message was sent
						<-messageSentNotificationChan
						// clear accumulator memory
						delete(clientEofs, clientID)
						delete(neededUsers, clientID)
						utils.RemoveClientDir(baseDir, clientID)
						utils.RemoveClientDir(baseDir+"/secondary", clientID)
					}
					continue
				}

				// Reset builder for reuse
				outBuilder.Reset()

				for _, transaction := range items {
					if concatenated, ok := concatTop3WithBirthdates(transaction, neededUsers[clientID]); ok {
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
				// Acknowledge message
				msg.Ack(false)
				log.Infof("Processed message")

			}
		}
	}
}
