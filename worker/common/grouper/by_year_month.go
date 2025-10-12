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
func parseTransactionItemData(transaction string) (string, ItemStats) {
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
	clientEofCount := map[string]int{}
	accumulator := make(map[string]map[string]ItemStats)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		for {
			select {
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
					accumulator[clientID] = make(map[string]ItemStats)
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
						batches := get_accumulator_batches(accumulator[clientID])
						for _, batch := range batches {
							outChan <- clientID + batch
						}
						msg := clientID + "\nEOF"
						outChan <- msg

						// clear accumulator memory
						accumulator[clientID] = nil
					}
					continue
				}

				for _, transaction := range transactions {
					item_id, item_tx_stat := parseTransactionItemData(transaction)
					if _, ok := accumulator[clientID][item_id]; !ok {
						// TODO: ACCUMULATOR MUST BE CLIENT SPECIFIC
						accumulator[clientID][item_id] = ItemStats{quantity: 0, subtotal: 0, date: item_tx_stat.date, id: item_tx_stat.id}
					}
					txStat := accumulator[clientID][item_id]
					txStat.quantity += item_tx_stat.quantity
					txStat.subtotal += item_tx_stat.subtotal
					accumulator[clientID][item_id] = txStat
				}
			}
		}
	}
}
