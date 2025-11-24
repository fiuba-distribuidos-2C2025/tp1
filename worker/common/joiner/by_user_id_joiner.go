package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

func concatTop3WithBirthdates(users []string, neededUsers map[string]string) string {
	var sb strings.Builder

	for _, user := range users {
		// user_id,gender,birthdate,registered_at
		// 2018392,male,1992-12-27,2025-06-01 09:56:51
		fields := strings.Split(user, ",")
		if len(fields) < 4 {
			log.Errorf("Invalid user format: %s", user)
			continue
		}
		userId := fields[0] + ".0" // TODO: datasets show userId in different formats
		birthdate := fields[2]
		if _, needed := neededUsers[userId]; needed {
			top3line := neededUsers[userId]
			elements := strings.Split(top3line, ",")
			if len(elements) < 3 {
				continue
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
			sb.WriteByte('\n')
		}
	}

	return sb.String()
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
	// Load existing clients EOF count in case of worker restart
	clientsEofCount, err := utils.LoadClientsEofCount(baseDir)
	if err != nil {
		log.Errorf("Error loading clients EOF count: %v", err)
		return nil
	}
	ResendClientEofs(clientsEofCount, neededEof, outChan, baseDir, workerID, messageSentNotificationChan)
	neededUsers := make(map[string]map[string]string)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("MESSAGE RECEIVED")
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]
				msgID := lines[1]

				// Ensure we have the needed users for this client.
				// This will block until the expected clientID arrives.
				// TODO: avoid blocking other clients while waiting for one
				for len(neededUsers[clientID]) == 0 {
					secondaryQueueNewMessage, ok := <-top3PerStoreRowsChan
					if !ok {
						log.Errorf("top3PerStoreRowsChan closed while waiting for client %s", clientID)
						return
					}

					log.Debugf("RECEIVED MSG FROM SECONDARY QUEUE, BEING INSIDE PRIMARY QUEUE:\n%s", secondaryQueueNewMessage)
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
					log.Warningf("Filled secondary queue data expected %s, got %s", clientID, secondaryQueueClientID)
				}

				users := lines[2:]
				if users[0] == "EOF" {
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
						// Use the workerID as msgID for the EOF
						// to ensure uniqueness across workers and restarts
						outChan <- clientID + "\n" + workerID + "\nEOF"
						// Here we just block until we are notified that the message was sent
						<-messageSentNotificationChan
						// clear accumulator memory
						delete(clientsEofCount, clientID)
						delete(neededUsers, clientID)
						utils.RemoveClientDir(baseDir, clientID)
						utils.RemoveClientDir(baseDir+"/secondary", clientID)

					}
					continue
				}

				top3WithBirthdates := concatTop3WithBirthdates(users, neededUsers[clientID])
				if top3WithBirthdates != "" {
					// Reuse the same msgID as it is already unique
					// and persistent across worker restarts
					outChan <- clientID + "\n" + msgID + "\n" + top3WithBirthdates
					// Here we just block until we are notified that the message was sent
					<-messageSentNotificationChan
				}

				// Acknowledge message
				msg.Ack(false)

			}
		}
	}
}
