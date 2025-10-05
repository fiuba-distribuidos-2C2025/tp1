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
	eofCount := 0
	accumulator := make(map[string]grouper.SemesterStats)
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
					log.Debugf("Received eof (%d/%d)", eofCount, neededEof)
					if eofCount >= neededEof {
						batches := grouper.GetSemesterAccumulatorBatches(accumulator)
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

					semester_key, sub_month_item_stats := parseSemesterStoreData(transaction)
					if _, ok := accumulator[semester_key]; !ok {
						accumulator[semester_key] = grouper.SemesterStats{Tpv: 0, Year: sub_month_item_stats.Year, YearHalf: sub_month_item_stats.YearHalf, StoreId: sub_month_item_stats.StoreId}
					}
					semesterStat := accumulator[semester_key]
					semesterStat.Tpv += sub_month_item_stats.Tpv
					accumulator[semester_key] = semesterStat
				}
			}
		}
	}
}
