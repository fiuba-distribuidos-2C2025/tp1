package grouper

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

type SemesterStats struct {
	Year     string
	YearHalf string
	StoreId  string
	Tpv      float64
}

// sample string input
// Sample transaction received:
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,100,2023-07-01 07:00:00
func parseTransactionData(transaction string) (string, SemesterStats) {
	// Parse CSV: transaction_id,item_id,quantity,unit_price,subtotal,created_at
	fields := strings.Split(transaction, ",")
	if len(fields) < 5 {
		log.Errorf("Invalid transaction format: %s", transaction)
		return "", SemesterStats{}
	}

	storeId := fields[1]

	// Resolve year half
	var year string
	var yearHalf string
	createdAt := fields[4]

	t, _ := time.Parse(time.DateTime, createdAt)
	if t.Month() <= 6 {
		yearHalf = "H1"
	} else {
		yearHalf = "H2"
	}
	if len(createdAt) >= 4 {
		year = createdAt[:4] // Extract first 4 characters (YYYY)
	}

	finalAmount, err := strconv.ParseFloat(fields[3], 64)
	if err != nil {
		log.Errorf("Invalid subtotal in transaction: %s", transaction)
		return "", SemesterStats{}
	}

	stats := SemesterStats{
		Tpv:      finalAmount,
		Year:     year,
		YearHalf: yearHalf,
		StoreId:  storeId,
	}

	return storeId + "-" + year + "-" + yearHalf, stats
}

// Convert accumulator map to batches of at most 10mb strings for output
func getSemesterAccumulatorBatches(accumulator map[string]SemesterStats) ([]string, []string) {
	const maxBatchSizeBytes = 10 * 1024 * 1024 // 10 MB

	var semesterKeys []string
	var batches []string

	// Track partial batches and sizes per semester
	semesterBuilders := make(map[string]*strings.Builder)
	semesterSizes := make(map[string]int)

	for key, stats := range accumulator {
		semesterKey := stats.Year + "-" + stats.YearHalf // This is the key used to distribute between aggregators later
		// Prepare line to write
		line := key + "," + stats.Year + "," + stats.YearHalf + "," +
			stats.StoreId + "," + strconv.FormatFloat(stats.Tpv, 'f', 2, 64) + "\n"
		lineSizeBytes := len(line)

		// Initialize structures if this semester hasn't been seen yet
		if _, exists := semesterBuilders[semesterKey]; !exists {
			semesterBuilders[semesterKey] = &strings.Builder{}
			semesterSizes[semesterKey] = 0
		}

		builder := semesterBuilders[semesterKey]
		currentSize := semesterSizes[semesterKey]

		// Flush this semester's batch if adding this line would exceed max size
		if currentSize+lineSizeBytes > maxBatchSizeBytes && currentSize > 0 {
			semesterKeys = append(semesterKeys, semesterKey)
			batches = append(batches, builder.String())
			builder.Reset()
			currentSize = 0
		}

		builder.WriteString(line)
		semesterSizes[semesterKey] = currentSize + lineSizeBytes
	}

	// Flush remaining non-empty semester batches
	for semesterKey, builder := range semesterBuilders {
		if semesterSizes[semesterKey] > 0 {
			semesterKeys = append(semesterKeys, semesterKey)
			batches = append(batches, builder.String())
		}
	}

	return semesterKeys, batches
}

// Function called when EOF threshold is reached for a client
func ThresholdReachedHandleSemester(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	accumulator := make(map[string]SemesterStats)

	messagesDir := filepath.Join(baseDir, clientID, "messages")

	entries, err := os.ReadDir(messagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages, forward EOF and clean up
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
			semester_key, tx_stat := parseTransactionData(transaction)
			if _, ok := accumulator[semester_key]; !ok {
				accumulator[semester_key] = SemesterStats{Tpv: 0, Year: tx_stat.Year, YearHalf: tx_stat.YearHalf, StoreId: tx_stat.StoreId}
			}
			txStat := accumulator[semester_key]
			txStat.Tpv += tx_stat.Tpv
			accumulator[semester_key] = txStat
		}
	}

	semesterKeys, batches := getSemesterAccumulatorBatches(accumulator)
	SendBatches(outChan, messageSentNotificationChan, clientID, workerID, batches, semesterKeys)

	// Use the workerID as msgID for the EOF
	// to ensure uniqueness across workers and restarts
	outChan <- clientID + "\n" + workerID + "\nEOF"

	// Here we just block until we are notified that the message was sent
	<-messageSentNotificationChan

	// clean up client directory
	return utils.RemoveClientDir(baseDir, clientID)
}
