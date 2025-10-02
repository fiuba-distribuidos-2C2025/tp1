package filterer

import (
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

// Combined validation and field extraction to avoid splitting twice
// transaction_id,item_id,quantity,unit_price,subtotal,created_at
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00
func filterAndExtractFieldsItems(transaction string, minYear int, maxYear int) bool {
	elements := strings.Split(transaction, ",")
	if len(elements) < 6 {
		return false
	}

	// Extract and validate year (field index 8)
	createdAt := elements[5]
	t, _ := time.Parse(time.DateTime, createdAt)
	// year := extractYear(createdAt)
	if t.Year() < minYear || t.Year() > maxYear {
		return false
	}
	return true
}

// Filter responsible for filtering transactions by year.
// Minimum year to filter transactions: 2024
// Maximum year to filter transactions: 2025
//
// Assumes it receives data in batches: csv rows separated by newlines.
//
// Sample row received:
//
// Output format of each row (batched when processed):
// transaction_id,item_id,quantity,unit_price,subtotal,created_at
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00
func CreateByYearFilterItemsCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		// Reusable buffer for building output
		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Info("PROCESSING MESSAGE")
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
						outChan <- "EOF"
					}
					continue
				}
				outBuilder.Reset()

				transactions := splitBatchInRows(body)
				for _, transaction := range transactions {
					if ok := filterAndExtractFieldsItems(transaction, minYearAllowed, maxYearAllowed); ok {
						outBuilder.WriteString(transaction)
						outBuilder.WriteByte('\n')
					}
				}

				if outBuilder.Len() > 0 {
					log.Info("MESSAGE OUT")
					outChan <- outBuilder.String()
				}
			}
		}
	}
}
