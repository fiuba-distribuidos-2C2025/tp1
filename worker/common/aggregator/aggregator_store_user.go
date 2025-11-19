package aggregator

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
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
func thresholdReachedHandleStoreUser(outChan chan string, baseDir string, clientID string) error {
	accumulator := make(map[string]grouper.UserStats)

	messagesDir := filepath.Join(baseDir, clientID, "messages")

	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages, forward EOF and clean up
			msg := clientID + "\nEOF"
			outChan <- msg
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

	for _, batch := range batches {
		if batch != "" {
			outChan <- clientID + "\n" + batch
		}
	}
	msg := clientID + "\nEOF"
	outChan <- msg
	// clean up client directory
	return utils.RemoveClientDir(baseDir, clientID)
}

func CreateByStoreUserAggregatorCallbackWithOutput(outChan chan string, neededEof int, baseDir string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	// Check existing EOF thresholds before starting to consume messages.
	// This ensures that if the worker restarts, it can pick up where it left off.
	// TODO: move this to Worker once all workers implement it
	err := utils.CheckAllClientsEOFThresholds(outChan, baseDir, neededEof, thresholdReachedHandleStoreUser)
	if err != nil {
		log.Errorf("Error checking existing EOF thresholds: %v", err)
		return nil
	}
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		for {
			select {
			case <-done:
				log.Info("Shutdown signal received, stopping worker...")
				return

			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.SplitN(payload, "\n", 3)

				// Separate header and the rest
				clientID := lines[0]
				msgId := lines[1]

				// Store message or EOF on disk
				if lines[2] == "EOF" {
					utils.StoreEOF(baseDir, clientID, msgId)
				} else {
					utils.StoreMessage(baseDir, clientID, msgId, lines[2])
				}

				// Acknowledge message
				msg.Ack(false)

				// Check if threshold reached for this client
				if lines[2] == "EOF" {
					thresholdReached, err := utils.ThresholdReached(baseDir, clientID, neededEof)
					if err != nil {
						log.Errorf("Error checking threshold for client %s: %v", clientID, err)
						return
					}
					if thresholdReached {
						err := thresholdReachedHandleStoreUser(outChan, baseDir, clientID)
						if err != nil {
							log.Errorf("Error handling threshold reached for client %s: %v", clientID, err)
							return
						}
					}
				}
			}
		}
	}
}
