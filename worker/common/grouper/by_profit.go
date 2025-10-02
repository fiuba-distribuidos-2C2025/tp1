package grouper

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

type ItemStats struct {
	id       string
	quantity int
	subtotal float64
	date     string
}

// sample string input
// transaction_id,item_id,quantity,unit_price,subtotal,created_at
// 3e02f6c2-fcf5-4e79-9bef-50f2a479f18d,5,3,9.0,27.0,2023-08-01 07:00:02
func parseTransactionData(transaction string) (string, ItemStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 6 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", ItemStats{}
	}

	itemId := fields[1]
	quantity, err := strconv.Atoi(fields[2])
	if err != nil {
		log.Errorf("Invalid quantity in transaction: %s", transaction)
		return "", ItemStats{}
	}

	subtotal, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		log.Errorf("Invalid subtotal in transaction: %s", transaction)
		return "", ItemStats{}
	}

	// Extract year-month from date (e.g., "2023-08-01 07:00:02" -> "2023-08")
	dateStr := fields[5]
	yearMonth := dateStr
	if len(dateStr) >= 7 {
		yearMonth = dateStr[:7] // Extract first 7 characters (YYYY-MM)
	}

	stats := ItemStats{
		id:       itemId,
		quantity: quantity,
		subtotal: subtotal,
		date:     yearMonth,
	}

	return yearMonth + "-" + itemId, stats
}

// Convert accumulator map to batches of at most 10mb strings for output
func get_accumulator_batches(accumulator map[string]ItemStats) []string {
	var batches []string
	var currentBatch strings.Builder
	currentSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10 MB

	for key, stats := range accumulator {
		line := key + "," + stats.date + "," + stats.id + "," + strconv.Itoa(stats.quantity) + "," + strconv.FormatFloat(stats.subtotal, 'f', 2, 64) + "\n"
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

func CreateByYearMonthGrouperCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	accumulator := make(map[string]ItemStats)
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
						batches := get_accumulator_batches(accumulator)
						for _, batch := range batches {
							outChan <- batch
						}
						outChan <- "EOF"
					}
					continue
				}

				transactions := splitBatchInRows(body)
				for _, transaction := range transactions {
					item_id, item_tx_stat := parseTransactionData(transaction)
					if _, ok := accumulator[item_id]; !ok {
						accumulator[item_id] = ItemStats{quantity: 0, subtotal: 0, id: item_tx_stat.id, date: item_tx_stat.date}
					}
					txStat := accumulator[item_id]
					txStat.quantity += item_tx_stat.quantity
					txStat.subtotal += item_tx_stat.subtotal
					accumulator[item_id] = txStat
				}
			}
		}
	}
}
