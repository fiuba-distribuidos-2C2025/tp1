package aggregator

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/grouper"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

// sample string input
// key,storeId,userId,purchasesQty
// 1-1,1,1,10
func parseStoreUserData(transaction string) (string, grouper.UserStats) {
	fields := strings.Split(transaction, ",")
	if len(fields) < 4 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", grouper.UserStats{}
	}

	key := fields[0]
	storeId := fields[1]
	userId := fields[2]
	if userId == "" {
		log.Errorf("Invalid userID in transaction: %s", transaction)
		return "", grouper.UserStats{}
	}

	purchasesQty, err := strconv.Atoi(fields[3])
	if err != nil {
		log.Errorf("Invalid purchasesQty in transaction: %s", transaction)
		return "", grouper.UserStats{}
	}

	stats := grouper.UserStats{
		UserId:       userId,
		StoreId:      storeId,
		PurchasesQty: purchasesQty,
	}

	return key, stats
}

func getTop3Users(accumulator map[string]grouper.UserStats) map[string][]grouper.UserStats {
	top3Accumulator := make(map[string][]grouper.UserStats)

	for _, stats := range accumulator {
		storeStats := top3Accumulator[stats.StoreId]
		storeStats = append(storeStats, stats)

		// Sort descending by PurchasesQty
		sort.Slice(storeStats, func(i, j int) bool {
			return storeStats[i].PurchasesQty > storeStats[j].PurchasesQty
		})

		// Keep only top 3
		if len(storeStats) > 3 {
			storeStats = storeStats[:3]
		}

		top3Accumulator[stats.StoreId] = storeStats
	}

	return top3Accumulator
}

// Convert accumulator map to batches of at most 10mb strings for output
func getUserAccumulatorBatches(maxQuantityItems map[string][]grouper.UserStats) []string {
	var batches []string
	var currentBatch strings.Builder
	currentSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10 MB

	for _, storeStats := range maxQuantityItems {
		for _, userStats := range storeStats {

			line := userStats.StoreId + "," + userStats.UserId + "," + strconv.Itoa(userStats.PurchasesQty) + "\n"
			lineSize := len(line)

			if currentSize+lineSize > maxBatchSize && currentSize > 0 {
				batches = append(batches, currentBatch.String())
				currentBatch.Reset()
				currentSize = 0
			}

			currentBatch.WriteString(line)
			currentSize += lineSize
		}
	}
	if currentSize > 0 {
		batches = append(batches, currentBatch.String())
	}

	return batches
}

// Function called when EOF threshold is reached for a client
func ThresholdReachedHandleStoreUser(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	accumulator := make(map[string]grouper.UserStats)

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

			store_user_key, sub_user_stats := parseStoreUserData(transaction)
			if store_user_key == "" {
				// log.Debugf("Invalid data in transaction: %s, ignoring", transaction)
				continue
			}

			if _, ok := accumulator[store_user_key]; !ok {
				accumulator[store_user_key] = grouper.UserStats{UserId: sub_user_stats.UserId, StoreId: sub_user_stats.StoreId, PurchasesQty: 0}
			}
			userStats := accumulator[store_user_key]
			userStats.PurchasesQty += sub_user_stats.PurchasesQty
			accumulator[store_user_key] = userStats
		}
	}

	top3Users := getTop3Users(accumulator)
	batches := getUserAccumulatorBatches(top3Users)

	for i, batch := range batches {
		if batch != "" {
			// This ensures deterministic message IDs per batch
			msgID := workerID + "-" + strconv.Itoa(i)
			outChan <- clientID + "\n" + msgID + "\n" + batch
			// Here we just block until we are notified that the message was sent
			<-messageSentNotificationChan
		}
	}
	// Use the workerID as msgID for the EOF
	// to ensure uniqueness across workers and restarts
	outChan <- clientID + "\n" + workerID + "\nEOF"
	// Here we just block until we are notified that the message was sent
	<-messageSentNotificationChan
	// clean up client directory
	return utils.RemoveClientDir(baseDir, clientID)
}
