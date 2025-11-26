package grouper

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

type UserStats struct {
	UserId       string
	StoreId      string
	PurchasesQty int
}

// sample string input
// transaction_id,store_id,user_id,final_amount,created_at
// 93da72d9-566a-45ed-b7d4-ce8c38430d94,9,1307362.0,18.0,2025-01-31 19:58:11
func parseTransactionUserData(transaction string) (string, UserStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 5 {
		return "", UserStats{}
	}

	storeId := fields[1]
	userID := fields[2]
	if userID == "" {
		return "", UserStats{}
	}

	stats := UserStats{
		UserId:       userID,
		StoreId:      storeId,
		PurchasesQty: 1,
	}

	return storeId + "-" + userID, stats
}

// Convert accumulator map to batches of at most 10mb strings for output
func getUserAccumulatorBatches(accumulator map[string]UserStats) ([]string, []string) {
	const maxBatchSizeBytes = 10 * 1024 * 1024 // 10 MB

	var userKeys []string
	var batches []string

	// Track partial batches and sizes per user
	userBuilders := make(map[string]*strings.Builder)
	userSizes := make(map[string]int)

	for key, stats := range accumulator {
		userKey := stats.StoreId // This is the key used to distribute between aggregators later

		// Prepare line to write
		line := key + "," + stats.StoreId + "," + stats.UserId + "," + strconv.Itoa(stats.PurchasesQty) + "\n"
		lineSizeBytes := len(line)

		// Initialize structures if this user hasn't been seen yet
		if _, exists := userBuilders[userKey]; !exists {
			userBuilders[userKey] = &strings.Builder{}
			userSizes[userKey] = 0
		}

		builder := userBuilders[userKey]
		currentSize := userSizes[userKey]

		// Flush this user's batch if adding this line would exceed max size
		if currentSize+lineSizeBytes > maxBatchSizeBytes && currentSize > 0 {
			userKeys = append(userKeys, userKey)
			batches = append(batches, builder.String())
			builder.Reset()
			currentSize = 0
		}

		builder.WriteString(line)
		userSizes[userKey] = currentSize + lineSizeBytes
	}

	// Flush remaining non-empty user batches
	for userKey, builder := range userBuilders {
		if userSizes[userKey] > 0 {
			userKeys = append(userKeys, userKey)
			batches = append(batches, builder.String())
		}
	}

	return userKeys, batches
}

// Function called when EOF threshold is reached for a client
func ThresholdReachedHandleByStoreUser(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	accumulator := make(map[string]UserStats)

	messagesDir := filepath.Join(baseDir, clientID, "messages")

	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages, forward EOF and clean up
			// Use the workerID as msgID for the EOF
			// to ensure uniqueness across workers and restarts
			outChan <- clientID + "\n" + workerID + "\nEOF"
			// Here we just block until we are notified that the message was sent
			<-messageSentNotificationChan
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
		if err != nil {
			log.Infof("failed to read file %s: %v", filePath, err)
			continue
		}

		payload := strings.TrimSpace(string(data))

		transactions := strings.Split(payload, "\n")

		for _, transaction := range transactions {
			store_user_key, user_stats := parseTransactionUserData(transaction)
			if store_user_key == "" {
				// log.Debugf("Invalid data in transaction: %s, ignoring", transaction)
				continue
			}
			if _, ok := accumulator[store_user_key]; !ok {
				accumulator[store_user_key] = UserStats{UserId: user_stats.UserId, StoreId: user_stats.StoreId, PurchasesQty: 0}
			}
			userStats := accumulator[store_user_key]
			userStats.PurchasesQty += 1
			accumulator[store_user_key] = userStats
		}
	}

	userKeys, batches := getUserAccumulatorBatches(accumulator)
	SendBatches(outChan, messageSentNotificationChan, clientID, workerID, batches, userKeys)
	// Use the workerID as msgID for the EOF
	// to ensure uniqueness across workers and restarts
	outChan <- clientID + "\n" + workerID + "\nEOF"

	// Here we just block until we are notified that the message was sent
	<-messageSentNotificationChan

	// clean up client directory
	return utils.RemoveClientDir(baseDir, clientID)
}
