package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

// Validates if a transaction final amount is greater than the target amount.
// Sample transaction received:
// key,year,yearHalf,storeId,tpv
// 1-2024-H1,2024,H1,1,100.3
func concatWithStoresData(transaction string, storesData map[string]string) (string, bool) {
	elements := strings.Split(transaction, ",")
	if len(elements) < 4 {
		return "", false
	}
	year := elements[1]
	yearHalf := elements[2]
	storeId := elements[3]
	tpv := elements[4]

	storeName, exists := storesData[storeId]
	if !exists {
		return "", false
	}

	var sb strings.Builder
	sb.WriteString(year)
	sb.WriteByte('-')
	sb.WriteString(yearHalf)
	sb.WriteByte(',')
	sb.WriteString(storeName)
	sb.WriteByte(',')
	sb.WriteString(tpv)

	return sb.String(), true
}

// example store ids data input
// store_id,store_name,city,state:
//
//	1,G Coffee @ USJ 89q,USJ 89q,Kuala Lumpur
func ProcessStoreIds(storesDataLines []string) map[string]string {
	// Preprocess stores data into a map for quick lookup
	storesData := make(map[string]string)
	for _, line := range storesDataLines {
		fields := strings.Split(line, ",")
		if len(fields) < 4 {
			log.Errorf("Invalid store data format: %s", line)
			continue
		}
		storeId := fields[0]
		storesData[storeId] = fields[1]
	}
	return storesData
}

func CreateByStoreIdJoinerCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, storeIdRowsChan chan string, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		// Load existing clients EOF count in case of worker restart
		clientEofs, err := utils.LoadClientsEofs(baseDir)
		if err != nil {
			log.Errorf("Error loading clients EOF count: %v", err)
			return
		}
		processedStores := make(map[string]map[string]string)
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case secondaryQueueNewMessage, ok := <-storeIdRowsChan:
				if !ok {
					log.Errorf("storeIdRowsChan closed with error %s", err)
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
				storeIds := rows[1:]
				processedStoreIds := ProcessStoreIds(storeIds)
				processedStores[secondaryQueueClientID] = processedStoreIds
				processPendingMessagesForClient(secondaryQueueClientID, processedStores, clientEofs, outChan, messageSentNotificationChan, baseDir, workerID, neededEof, concatWithStoresData)
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
				if _, exists := processedStores[clientID]; !exists {
					processedStores[clientID] = make(map[string]string)
				}

				if len(processedStores[clientID]) == 0 {
					log.Infof("Stores data not yet available for client %s, storing message...", clientID)
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
						delete(processedStores, clientID)
						utils.RemoveClientDir(baseDir, clientID)
						utils.RemoveClientDir(baseDir+"/secondary", clientID)

					}
					continue
				}

				// Reset builder for reuse
				outBuilder.Reset()

				for _, transaction := range items {
					if concatenated, ok := concatWithStoresData(transaction, processedStores[clientID]); ok {
						outBuilder.WriteString(concatenated)
						outBuilder.WriteByte('\n')
					}
				}

				if outBuilder.Len() > 0 {
					// Reuse the same msgID as it is already unique
					// and persistent across worker restarts
					outChan <- clientID + "\n" + msgID + "\n" + outBuilder.String()
					// Here we just block until we are notified that the message was sent
					<-messageSentNotificationChan
				}
				// Acknowledge message
				msg.Ack(false)
				log.Infof("Processed message")
			}
		}
	}
}
