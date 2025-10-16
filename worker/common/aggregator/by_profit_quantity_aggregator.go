package aggregator

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
// key,year-month,itemId,quantity,subtotal
// 2023-08-5,2023-08,5,10,27.0
func parseTransactionData(transaction string) (string, ItemStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 4 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", ItemStats{}
	}

	key := fields[0]
	yearMonth := fields[1]

	itemId := fields[2]
	quantity, err := strconv.Atoi(fields[3])
	if err != nil {
		log.Errorf("Invalid quantity in transaction: %s", transaction)
		return "", ItemStats{}
	}

	subtotal, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		log.Errorf("Invalid subtotal in transaction: %s", transaction)
		return "", ItemStats{}
	}

	stats := ItemStats{
		id:       itemId,
		quantity: quantity,
		subtotal: subtotal,
		date:     yearMonth,
	}

	return key, stats
}

func getMaxItems(accumulator map[string]ItemStats) (map[string]ItemStats, map[string]ItemStats) {
	var maxQuantityAccumulator map[string]ItemStats = make(map[string]ItemStats)
	var maxProfitAccumulator map[string]ItemStats = make(map[string]ItemStats)

	for _, stats := range accumulator {
		// Check and update max quantity per month
		if existing, ok := maxQuantityAccumulator[stats.date]; !ok || stats.quantity > existing.quantity {
			maxQuantityAccumulator[stats.date] = stats
		}
		// Check and update max profit per month
		if existing, ok := maxProfitAccumulator[stats.date]; !ok || stats.subtotal > existing.subtotal {
			maxProfitAccumulator[stats.date] = stats
		}
	}
	return maxQuantityAccumulator, maxProfitAccumulator
}

// Convert accumulator map to batches of at most 10mb strings for output
func get_accumulator_batches(maxQuantityItems map[string]ItemStats, maxProfitItems map[string]ItemStats) []string {
	var batches []string
	var currentBatch strings.Builder
	currentSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10 MB

	for _, stats := range maxQuantityItems {
		line := "QUANTITY" + "," + stats.date + "," + stats.id + "," + strconv.Itoa(stats.quantity) + "\n"
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
		currentBatch.Reset()
		currentSize = 0
	}

	for _, stats := range maxProfitItems {
		line := "PROFIT" + "," + stats.date + "," + stats.id + "," + strconv.FormatFloat(stats.subtotal, 'f', 2, 64) + "\n"
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

func CreateByQuantityProfitAggregatorCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
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
					log.Debugf("Received eof (%d/%d) for client %d", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						maxQuantityItems, maxProfitITems := getMaxItems(accumulator[clientID])

						batches := get_accumulator_batches(maxQuantityItems, maxProfitITems)
						for _, batch := range batches {
							if batch != "" {
								outChan <- clientID + "\n" + batch
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

					item_key, sub_month_item_stats := parseTransactionData(transaction)
					if _, ok := accumulator[clientID][item_key]; !ok {
						accumulator[clientID][item_key] = ItemStats{quantity: 0, subtotal: 0, id: sub_month_item_stats.id, date: sub_month_item_stats.date}
					}
					txStat := accumulator[clientID][item_key]
					txStat.quantity += sub_month_item_stats.quantity
					txStat.subtotal += sub_month_item_stats.subtotal
					accumulator[clientID][item_key] = txStat
				}
			}
		}
	}
}
