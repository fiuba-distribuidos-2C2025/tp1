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

func CreateByUserStoreIdJoinerCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, storeIdRowsChan chan string, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return CreatePrimaryQueueCallbackWithOutput(outChan, storeIdRowsChan, ProcessStoreIds, neededEof, baseDir, workerID, messageSentNotificationChan, concatBirthdatesWithStoresData)
}
