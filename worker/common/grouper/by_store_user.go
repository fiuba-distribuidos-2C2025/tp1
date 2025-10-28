package grouper

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
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

func CreateByStoreUserGrouperCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	accumulator := make(map[string]map[string]UserStats)
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

				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]

				// Create accumulator for client
				if _, exists := accumulator[clientID]; !exists {
					accumulator[clientID] = make(map[string]UserStats)
				}

				transactions := lines[1:]
				if transactions[0] == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					log.Debugf("Received eof (%d/%d)", eofCount, neededEof)
					if eofCount >= neededEof {
						userKeys, batches := getUserAccumulatorBatches(accumulator[clientID])
						for i, batch := range batches {
							if batch != "" {
								outChan <- clientID + "\n" + userKeys[i] + "\n" + batch
							}
						}
						msg := clientID + "\nEOF"
						outChan <- msg

						// clear accumulator memory
						accumulator[clientID] = nil
					}
					continue
				}

				for _, transaction := range transactions {
					store_user_key, user_stats := parseTransactionUserData(transaction)
					if store_user_key == "" {
						// log.Debugf("Invalid data in transaction: %s, ignoring", transaction)
						continue
					}
					if _, ok := accumulator[clientID][store_user_key]; !ok {
						accumulator[clientID][store_user_key] = UserStats{UserId: user_stats.UserId, StoreId: user_stats.StoreId, PurchasesQty: 0}
					}
					userStats := accumulator[clientID][store_user_key]
					userStats.PurchasesQty += 1
					accumulator[clientID][store_user_key] = userStats
				}
			}
		}
	}
}
