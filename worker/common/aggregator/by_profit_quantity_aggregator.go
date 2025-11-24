package aggregator

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
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

// Function called when EOF threshold is reached for a client
func ThresholdReachedHandleProfitQuantity(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	accumulator := make(map[string]ItemStats)
	messagesDir := filepath.Join(baseDir, clientID, "messages")

	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		if os.IsNotExist(err) {
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

			item_key, sub_month_item_stats := parseTransactionData(transaction)
			if _, ok := accumulator[item_key]; !ok {
				accumulator[item_key] = ItemStats{quantity: 0, subtotal: 0, id: sub_month_item_stats.id, date: sub_month_item_stats.date}
			}
			txStat := accumulator[item_key]
			txStat.quantity += sub_month_item_stats.quantity
			txStat.subtotal += sub_month_item_stats.subtotal
			accumulator[item_key] = txStat
		}
	}

	maxQuantityItems, maxProfitITems := getMaxItems(accumulator)

	batches := get_accumulator_batches(maxQuantityItems, maxProfitITems)
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
