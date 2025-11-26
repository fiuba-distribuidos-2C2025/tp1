package grouper

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
func getAccumulatorBatches(accumulator map[string]ItemStats) ([]string, []string) {
	const maxBatchSizeBytes = 10 * 1024 * 1024 // 10 MB

	var itemKeys []string
	var batches []string

	// Track partial batches and sizes per item
	itemBuilders := make(map[string]*strings.Builder)
	itemSizes := make(map[string]int)

	for key, stats := range accumulator {
		itemKey := stats.date // This is the key used to distribute between aggregators later

		// Prepare line to write
		line := key + "," + stats.date + "," + stats.id + "," + strconv.Itoa(stats.quantity) + "," + strconv.FormatFloat(stats.subtotal, 'f', 2, 64) + "\n"
		lineSizeBytes := len(line)

		// Initialize structures if this item hasn't been seen yet
		if _, exists := itemBuilders[itemKey]; !exists {
			itemBuilders[itemKey] = &strings.Builder{}
			itemSizes[itemKey] = 0
		}

		builder := itemBuilders[itemKey]
		currentSize := itemSizes[itemKey]

		// Flush this item's batch if adding this line would exceed max size
		if currentSize+lineSizeBytes > maxBatchSizeBytes && currentSize > 0 {
			itemKeys = append(itemKeys, itemKey)
			batches = append(batches, builder.String())
			builder.Reset()
			currentSize = 0
		}

		builder.WriteString(line)
		itemSizes[itemKey] = currentSize + lineSizeBytes
	}

	// Flush remaining non-empty item batches
	for itemKey, builder := range itemBuilders {
		if itemSizes[itemKey] > 0 {
			itemKeys = append(itemKeys, itemKey)
			batches = append(batches, builder.String())
		}
	}

	return itemKeys, batches
}

// Function called when EOF threshold is reached for a client
func ThresholdReachedHandleByYearMonth(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	accumulator := make(map[string]ItemStats)

	messagesDir := filepath.Join(baseDir, clientID, "messages")

	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages, forward EOF and clean up
			return utils.SendEof(outChan, messageSentNotificationChan, baseDir, clientID, workerID)
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
			item_id, item_tx_stat := parseTransactionItemData(transaction)
			if _, ok := accumulator[item_id]; !ok {
				accumulator[item_id] = ItemStats{quantity: 0, subtotal: 0, date: item_tx_stat.date, id: item_tx_stat.id}
			}
			txStat := accumulator[item_id]
			txStat.quantity += item_tx_stat.quantity
			txStat.subtotal += item_tx_stat.subtotal
			accumulator[item_id] = txStat
		}
	}

	itemKeys, batches := getAccumulatorBatches(accumulator)

	SendBatches(outChan, messageSentNotificationChan, clientID, workerID, batches, itemKeys)

	return utils.SendEof(outChan, messageSentNotificationChan, baseDir, clientID, workerID)
}
