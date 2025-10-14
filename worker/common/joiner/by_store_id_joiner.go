package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
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

func CreateByStoreIdJoinerCallbackWithOutput(outChan chan string, neededEof int, storeIdRowsChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	processedStores := make(map[string]map[string]string)
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

				// Ensure we have stores for this client.
				// This will block until the expected clientID arrives.
				for len(processedStores[clientID]) == 0 {
					secondaryQueueNewMessage, ok := <-storeIdRowsChan
					if !ok {
						log.Errorf("storeIdRowsChan closed while waiting for client %s", clientID)
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
					storeIds := rows[1:]
					processedStoreIds := ProcessStoreIds(storeIds)
					processedStores[secondaryQueueClientID] = processedStoreIds

					log.Warningf("Filled secondary queue data expected %s, got %s", clientID, secondaryQueueClientID)
				}

				// Ack message only if we have secondary queue data to handle it
				msg.Ack(false)

				items := lines[1:]
				if len(items) > 0 && items[0] == "EOF" {
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

				outBuilder.WriteString(clientID + "\n")
				for _, transaction := range items {
					if concatenated, ok := concatWithStoresData(transaction, processedStores[clientID]); ok {
						outBuilder.WriteString(concatenated)
						outBuilder.WriteByte('\n')
					}
				}

				if outBuilder.Len() > 0 {
					outChan <- outBuilder.String()
					log.Infof("Processed message")
				}
			}
		}
	}
}
