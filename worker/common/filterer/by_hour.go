package filterer

import (
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

var byHourFieldIndices = []int{0, 1, 2, 3, 4}
var minHour = "06:00:00"
var maxHour = "23:00:00"

// Validates if a transaction is within the specified hour range.
// Sample transaction received:
// 2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,100,2023-07-01 07:00:00
func transactionInHourRange(transaction string, minHour string, maxHour string, indices []int) (string, bool) {
	elements := strings.Split(transaction, ",")
	if len(elements) < 5 {
		return "", false
	}
	createdAtTime := strings.Split(elements[4], " ")
	if len(createdAtTime) != 2 {
		return "", false
	}
	timestamp, err := time.Parse(time.TimeOnly, createdAtTime[1])
	if err != nil {
		return "", false
	}
	minHourParsed, err := time.Parse(time.TimeOnly, minHour)
	if err != nil {
		return "", false
	}
	maxHourParsed, err := time.Parse(time.TimeOnly, maxHour)
	if err != nil {
		return "", false
	}

	if timestamp.Before(minHourParsed) || timestamp.After(maxHourParsed) {
		return "", false
	}

	var sb strings.Builder
	for i, idx := range indices {
		elem := elements[idx]
		if idx <= len(elements)-1 {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(elem)
		}
	}
	return sb.String(), true
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
func CreateByHourFilterCallbackWithOutput(outChan chan string, messageSent chan string, neededEof int, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	filterFunc := func(transaction string) (string, bool) {
		return transactionInHourRange(transaction, minHour, maxHour, byHourFieldIndices)
	}
	return CreateGenericFilterCallbackWithOutput(outChan, messageSent, neededEof, filterFunc, baseDir, workerID)
}
