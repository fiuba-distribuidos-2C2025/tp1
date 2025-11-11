package aggregator

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/grouper"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

// sample string input
// key,year,yearHalf,storeId,tpv
// 1-2024-H1,2024,H1,1,100.3
func parseSemesterStoreData(transaction string) (string, grouper.SemesterStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 5 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", grouper.SemesterStats{}
	}

	key := fields[0]
	year := fields[1]
	yearHalf := fields[2]
	storeId := fields[3]
	tpv, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		log.Errorf("Invalid tpv in transaction: %s", transaction)
		return "", grouper.SemesterStats{}
	}

	stats := grouper.SemesterStats{
		Tpv:      tpv,
		Year:     year,
		YearHalf: yearHalf,
		StoreId:  storeId,
	}

	return key, stats
}

// Convert accumulator map to batches of at most 10mb strings for output
func getSemesterAccumulatorBatches(accumulator map[string]grouper.SemesterStats) []string {
	var batches []string
	var currentBatch strings.Builder
	currentSize := 0
	maxBatchSize := 10 * 1024 * 1024 // 10 MB

	for key, stats := range accumulator {
		line := key + "," + stats.Year + "," + stats.YearHalf + "," + stats.StoreId + "," + strconv.FormatFloat(stats.Tpv, 'f', 2, 64) + "\n"
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
func thresholdReachedHandle(outChan chan string, baseDir string, clientID string) error {
	accumulator := make(map[string]grouper.SemesterStats)

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
			semester_key, sub_month_item_stats := parseSemesterStoreData(transaction)
			if _, ok := accumulator[semester_key]; !ok {
				accumulator[semester_key] = grouper.SemesterStats{Tpv: 0, Year: sub_month_item_stats.Year, YearHalf: sub_month_item_stats.YearHalf, StoreId: sub_month_item_stats.StoreId}
			}
			semesterStat := accumulator[semester_key]
			semesterStat.Tpv += sub_month_item_stats.Tpv
			accumulator[semester_key] = semesterStat
		}
	}

	batches := getSemesterAccumulatorBatches(accumulator)
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
func CreateBySemesterAggregatorCallbackWithOutput(outChan chan string, neededEof int, baseDir string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	// Check existing EOF thresholds before starting to consume messages.
	// This ensures that if the worker restarts, it can pick up where it left off.
	// TODO: move this to Worker once all workers implement it
	err := utils.CheckAllClientsEOFThresholds(outChan, baseDir, neededEof, thresholdReachedHandle)
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
						err := thresholdReachedHandle(outChan, baseDir, clientID)
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
