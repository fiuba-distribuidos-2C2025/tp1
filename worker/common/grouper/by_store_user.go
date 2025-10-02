package grouper

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

type UserStats struct {
	userId        string
	storeId       string
	purchases_qty int
}

// sample string input
// transaction_id,store_id,user_id,final_amount,created_at
// 3e02f6c2-fcf5-4e79-9bef-50f2a479f18d,5,3,27.0,2023-08-01 07:00:02
func parseTransactionUserData(transaction string) (string, UserStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 6 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", UserStats{}
	}

	storeId := fields[1]
	userID := fields[2]

	stats := UserStats{
		userId:        userID,
		storeId:       storeId,
		purchases_qty: 1,
	}

	return storeId + userID, stats
}

// Convert accumulator map to batches of at most 10mb strings for output
func getUserAccumulatorBatches(accumulator map[string]UserStats) []string {
	var batches []string
	var currentBatch strings.Builder
	currentSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10 MB

	for key, stats := range accumulator {
		line := key + "," + stats.storeId + "," + stats.userId + "," + strconv.Itoa(stats.purchases_qty) + "\n"
		lineSize := len(line)

		if currentSize+lineSize > maxBatchSize && currentSize > 0 {
			batches = append(batches, currentBatch.String())
			currentBatch.Reset()
			currentSize = 0
		}

		currentBatch.WriteString(line)
		currentSize += lineSize
	}
	if currentSize > 0 {
		batches = append(batches, currentBatch.String())
	}

	return batches
}

func CreateByStoreUserGrouperCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	accumulator := make(map[string]UserStats)
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
						batches := getUserAccumulatorBatches(accumulator)
						for _, batch := range batches {
							outChan <- batch
						}
						outChan <- "EOF"
					}
					continue
				}

				transactions := splitBatchInRows(body)
				for _, transaction := range transactions {
					store_user_key, user_stats := parseTransactionUserData(transaction)
					if _, ok := accumulator[store_user_key]; !ok {
						accumulator[store_user_key] = UserStats{userId: user_stats.userId, storeId: user_stats.storeId, purchases_qty: 0}
					}
					userStats := accumulator[store_user_key]
					userStats.purchases_qty += 1
					accumulator[store_user_key] = userStats
				}
			}
		}
	}
}
