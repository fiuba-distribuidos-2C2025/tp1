package aggregator

import (
	"sort"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/grouper"
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

func CreateByStoreUserAggregatorCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	accumulator := make(map[string]grouper.UserStats)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		for {
			select {
			// TODO: Something will be wrong and notified here!
			// case <-done:
			// log.Info("Shutdown signal received, stopping worker...")
			// return

			case msg, ok := <-*consumeChannel:
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				body := strings.TrimSpace(string(msg.Body))

				if body == "EOF" {
					eofCount++
					log.Debug("Received eof (%d/%d)", eofCount, neededEof)
					if eofCount >= neededEof {
						top3Users := getTop3Users(accumulator)

						batches := getUserAccumulatorBatches(top3Users)
						for _, batch := range batches {
							outChan <- batch
						}
						outChan <- "EOF"

						// clear accumulator memory
						accumulator = nil
					}
					continue
				}

				transactions := splitBatchInRows(body)
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
		}
	}
}
