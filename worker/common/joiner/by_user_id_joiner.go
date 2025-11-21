package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

func concatTop3(top3PerStoreArray []string, usersBirthdays map[string]string) string {
	var sb strings.Builder

	for _, line := range top3PerStoreArray {
		elements := strings.Split(line, ",")
		if len(elements) < 3 {
			continue
		}
		// storeId,userId,PurchaseQty:
		// 1,1,10
		storeId := elements[0]
		userId := elements[1]
		purchaseQty := elements[2]

		// If we don't hold the data internally, we assume that
		// other worked does
		birthdate, exists := usersBirthdays[userId]
		if birthdate == "" || !exists {
			continue
		}

		sb.WriteString(storeId)
		sb.WriteByte(',')
		sb.WriteString(birthdate)
		sb.WriteByte(',')
		sb.WriteString(purchaseQty)
		sb.WriteByte('\n')
	}

	return sb.String()
}

// example store ids data input
// storeId,userId,PurchaseQty:
// 1,1,10
func processNeededUsers(top3Lines []string) (map[string]string, []string) {
	// Preprocess stores data into a map for quick lookup
	neededUsers := make(map[string]string)
	for _, line := range top3Lines {
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			log.Errorf("Invalid top 3 data format: %s", line)
			continue
		}
		userId := fields[1]
		neededUsers[userId] = ""
	}
	return neededUsers, top3Lines
}

func CreateByUserIdJoinerCallbackWithOutput(outChan chan string, neededEof int, top3PerStoreRowsChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	neededUsers := make(map[string]map[string]string)
	top3PerStoreArray := make(map[string][]string)
	// neededUsers, top3PerStoreArray := processNeededUsers(top3PerStoreRows)
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

				// Ensure we have the needed users for this client.
				// This will block until the expected clientID arrives.
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
					top3PerStoreRows := rows[1:]
					clientNeededUsers, clientTop3PerStoreArray := processNeededUsers(top3PerStoreRows)
					neededUsers[secondaryQueueClientID] = clientNeededUsers
					top3PerStoreArray[secondaryQueueClientID] = clientTop3PerStoreArray

					log.Warningf("Filled secondary queue data expected %s, got %s", clientID, secondaryQueueClientID)
				}

				// Ack message only if we have secondary queue data to handle it
				msg.Ack(false)

				users := lines[2:]
				if users[0] == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						clientTop3 := concatTop3(top3PerStoreArray[clientID], neededUsers[clientID])
						if clientTop3 != "" {
							outChan <- clientID + "\n" + clientTop3
						}
						outChan <- clientID + "\nEOF"
						// clear accumulator memory
						delete(clientEofCount, clientID)
						delete(neededUsers, clientID)
						delete(top3PerStoreArray, clientID)
					}
					continue
				}

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
					if _, needed := neededUsers[clientID][userId]; needed {
						neededUsers[clientID][userId] = birthdate
					}
				}
			}
		}
	}
}
