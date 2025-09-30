package filterer

import (
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

const FIELDS_IN_TRANSACTION = 5

// Validates if a transaction is within the specified hour range.
// Sample transaction received:
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,100,2023-07-01 07:00:00
func transactionInHourRange(transaction string, minHour string, maxHour string) bool {
	elements := strings.Split(transaction, ",")
	if len(elements) < FIELDS_IN_TRANSACTION {
		log.Info("Error parsing transaction: invalid format")
		return false
	}
	createdAtTime := strings.Split(elements[4], " ")
	if len(createdAtTime) != 2 {
		log.Info("Error parsing transaction: invalid format")
		return false
	}
	timestamp, err := time.Parse(time.TimeOnly, createdAtTime[1])
	if err != nil {
		log.Info("Error parsing transaction: invalid format")
		return false
	}
	minHourParsed, err := time.Parse(time.TimeOnly, minHour)
	if err != nil {
		return false
	}
	maxHourParsed, err := time.Parse(time.TimeOnly, maxHour)
	if err != nil {
		return false
	}
	return (timestamp.After(minHourParsed) || timestamp.Equal(minHourParsed)) && (timestamp.Before(maxHourParsed) || timestamp.Equal(maxHourParsed))
}

// Filter responsible for filtering transactions by hour.
// Minimum hour to filter transactions: 06:00:00
// Maximum hour to filter transactions: 23:00:00
//
// Assumes it receives data in batches: csv rows separated by newlines.
//
// Sample row received:
// transaction_id,store_id,user_id,final_amount,created_at
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,38.0,2023-07-01 07:00:00
//
// Output format of each row (batched when processed):
// transaction_id,store_id,user_id,final_amount,created_at
func CreateByHourFilterCallbackWithOutput(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
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
					if transactionInHourRange(transaction, "06:00:00", "23:00:00") {
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
