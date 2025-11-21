package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

// Sample transaction received:
// storeId,birthdate,purchaseQty
// 1,1990-01-01,10
func concatBirthdatesWithStoresData(transaction string, storesData map[string]string) (string, bool) {
	elements := strings.Split(transaction, ",")
	if len(elements) < 3 {
		return "", false
	}
	storeId := elements[0]
	birthdate := elements[1]
	purchaseQty := elements[2]

	storeName, exists := storesData[storeId]
	if !exists {
		return "", false
	}

	var sb strings.Builder
	sb.WriteString(storeName)
	sb.WriteByte(',')
	sb.WriteString(birthdate)
	sb.WriteByte(',')
	sb.WriteString(purchaseQty)

	return sb.String(), true
}

func CreateByUserStoreIdJoinerCallbackWithOutput(outChan chan string, neededEof int, storeIdRowsChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	processedStores := make(map[string]map[string]string)
	// processedStores := ProcessStoreIds(storeIdRows)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}

				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]

				// Create accumulator for client
				if _, exists := processedStores[clientID]; !exists {
					processedStores[clientID] = make(map[string]string)
				}

				// Ensure we have store ids for this client.
				// This will block until the expected clientID arrives.
				for len(processedStores[clientID]) == 0 {
					secondaryQueueNewMessage, ok := <-storeIdRowsChan
					if !ok {
						log.Errorf("menuItemRowsChan closed while waiting for client %s", clientID)
						return
					}

					log.Debugf("RECEIVED MSG FROM SECONDARY QUEUE, BEING INSIDE PRIMARY QUEUE:\n%s", secondaryQueueNewMessage)
					payload := strings.TrimSpace(secondaryQueueNewMessage)
					rows := strings.Split(payload, "\n")
					if len(rows) < 3 {
						log.Errorf("Invalid secondary queue payload (need header + at least one row): %q", payload)
						continue
					}

					secondaryQueueClientID := rows[0]
					storeIdRows := rows[2:]
					processedStoreIds := ProcessStoreIds(storeIdRows)
					processedStores[secondaryQueueClientID] = processedStoreIds

					log.Warningf("Filled secondary queue data expected %s, got %s", clientID, secondaryQueueClientID)
				}

				// Ack message only if we have secondary queue data to handle it
				msg.Ack(false)

				items := lines[2:]
				if items[0] == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						msg := clientID + "\nEOF"
						outChan <- msg
						// clear accumulator memory
						delete(clientEofCount, clientID)
						delete(processedStores, clientID)
					}
					continue
				}

				// Reset builder for reuse
				outBuilder.Reset()
				for _, transaction := range items {
					if concatenated, ok := concatBirthdatesWithStoresData(transaction, processedStores[clientID]); ok {
						outBuilder.WriteString(concatenated)
						outBuilder.WriteByte('\n')
					}
				}

				if outBuilder.Len() > 0 {
					outChan <- clientID + "\n" + outBuilder.String()
				}
				log.Infof("Processed message")
			}
		}
	}
}
