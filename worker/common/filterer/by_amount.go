package filterer

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

// Validates if a transaction final amount is greater than the target amount.
// Sample transaction received:
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,100,2023-07-01 07:00:00
func transactionGreaterFinalAmount(transaction string, targetAmount float64) bool {
	elements := strings.Split(transaction, ",")
	if len(elements) < 5 {
		return false
	}
	finalAmount, err := strconv.ParseFloat(strings.TrimSpace(elements[3]), 64)
	if err != nil {
		return false
	}
	return finalAmount >= targetAmount
}

// Filter responsible for filtering transactions by amount.
// Target amount to filter transactions: 75
//
// Assumes it receives data in batches: csv rows separated by newlines.
//
// Sample row received:
// transaction_id,store_id,user_id,final_amount,created_at
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,2023-07-01 07:00:00
//
// Output format of each row (batched when processed):
// transaction_id,final_amount
func CreateByAmountFilterCallbackWithOutput(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
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
				transactions := splitBatchInRows(body)

				outMsg := ""
				for _, transaction := range transactions {
					if transactionGreaterFinalAmount(transaction, 75) {
						indices := []int{0, 3}
						transaction := removeNeedlessFields(transaction, indices)
						outMsg += transaction + "\n"
					}
				}

				msg.Ack(false)
				if outMsg != "" {
					outChan <- outMsg
				}
			}
		}
	}
}
