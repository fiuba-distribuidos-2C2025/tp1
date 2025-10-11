package grouper

import (
	"strconv"
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
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
func GetSemesterAccumulatorBatches(accumulator map[string]SemesterStats) []string {
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

func CreateBySemesterGrouperCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	accumulator := make(map[string]SemesterStats)
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
						batches := GetSemesterAccumulatorBatches(accumulator)
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
					semester_key, tx_stat := parseTransactionData(transaction)
					if _, ok := accumulator[semester_key]; !ok {
						accumulator[semester_key] = SemesterStats{Tpv: 0, Year: tx_stat.Year, YearHalf: tx_stat.YearHalf, StoreId: tx_stat.StoreId}
					}
					txStat := accumulator[semester_key]
					txStat.Tpv += tx_stat.Tpv
					accumulator[semester_key] = txStat
				}
			}
		}
	}
}
