package filterer

import (
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

var byYearFieldIndices = []int{0, 1, 4, 7, 8}
var minYearAllowed = 2024
var maxYearAllowed = 2025

// Combined validation and field extraction to avoid splitting twice
func filterAndExtractFields(transaction string, minYear int, maxYear int, indices []int) (string, bool) {
	elements := strings.Split(transaction, ",")
	if len(elements) < 9 {
		return "", false
	}

	// Extract and validate year (field index 8)
	createdAt := elements[8]
	t, _ := time.Parse(time.DateTime, createdAt)
	// year := extractYear(createdAt)
	if t.Year() < minYear || t.Year() > maxYear {
		return "", false
	}

	// Build output with only needed fields
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
func CreateByYearFilterCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	filterFunc := func(transaction string) (string, bool) {
		return filterAndExtractFields(transaction, minYearAllowed, maxYearAllowed, byYearFieldIndices)
	}
	return CreateGenericFilterCallbackWithOutput(outChan, neededEof, filterFunc)
}
