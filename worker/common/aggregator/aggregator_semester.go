package aggregator

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/grouper"
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

func CreateBySemesterAggregatorCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	accumulator := make(map[string]map[string]grouper.SemesterStats)
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
					accumulator[clientID] = make(map[string]grouper.SemesterStats)
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
						batches := grouper.GetSemesterAccumulatorBatches(accumulator[clientID])
						for _, batch := range batches {
							outChan <- clientID + "\n" + batch
						}
						msg := clientID + "\nEOF"
						outChan <- msg

						// clear accumulator memory
						accumulator[clientID] = nil
					}
					continue
				}

				for _, transaction := range transactions {

					semester_key, sub_month_item_stats := parseSemesterStoreData(transaction)
					if _, ok := accumulator[clientID][semester_key]; !ok {
						accumulator[clientID][semester_key] = grouper.SemesterStats{Tpv: 0, Year: sub_month_item_stats.Year, YearHalf: sub_month_item_stats.YearHalf, StoreId: sub_month_item_stats.StoreId}
					}
					semesterStat := accumulator[clientID][semester_key]
					semesterStat.Tpv += sub_month_item_stats.Tpv
					accumulator[clientID][semester_key] = semesterStat
				}
			}
		}
	}
}
