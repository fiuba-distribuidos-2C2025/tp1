package filterer

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

// Validates if a transaction is within the specified year range.
// Sample transaction received:
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00
func transactionInYearRange(transaction string, minYear int, maxYear int) bool {
	elements := strings.Split(transaction, ",")
	if len(elements) < 9 {
		return false
	}
	createdAt := elements[8]
	year := createdAt[:4]
	return year >= strconv.Itoa(minYear) && year <= strconv.Itoa(maxYear)
}

// Filter responsible for filtering transactions by year.
// Minimum year to filter transactions: 2024
// Maximum year to filter transactions: 2025
//
// Assumes it receives data in batches: csv rows separated by newlines.
//
// Sample row received:
// transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00
//
// Output format of each row (batched when processed):
// transaction_id,store_id,user_id,final_amount,created_at
func CreateByYearFilterCallbackWithOutput(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		for {
			select {
			// TODO: Something will be wrong and notified here!
			// case <-done:
			// log.Info("Shutdown signal received, stopping worker...")
			// return

			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				body := strings.TrimSpace(string(msg.Body))
				transactions := messageToArray(body)

				outMsg := ""
				for _, transaction := range transactions {
					if transactionInYearRange(transaction, 2024, 2025) {
						// Keeps elements of index 0, 1, 4, 7, 8
						indices := []int{0, 1, 4, 7, 8}
						transaction := removeNeedlessFields(transaction, indices)
						outMsg += transaction + "\n"
					}
				}

				if outMsg != "" {
					outChan <- outMsg
				}
			}
		}
	}
}
