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
func ProcessStoreIds(storeRows string) map[string]string {
	// Preprocess stores data into a map for quick lookup
	storesData := make(map[string]string)
	storesDataLines := splitBatchInRows(storeRows)
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

func CreateByStoreIdJoinerCallbackWithOutput(outChan chan string, neededEof int, storeIdRows string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	processedStores := ProcessStoreIds(storeIdRows)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("MESSAGE RECEIVED")
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				body := strings.TrimSpace(string(msg.Body))

				if body == "EOF" {
					eofCount++
					if eofCount >= neededEof {
						outChan <- "EOF"
						continue
					}
				}

				// Reset builder for reuse
				transactions := splitBatchInRows(body)
				outBuilder.Reset()

				for _, transaction := range transactions {
					if concatenated, ok := concatWithStoresData(transaction, processedStores); ok {
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
