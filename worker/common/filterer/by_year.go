package filterer

import (
	"strings"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

var transactionFieldIndices = []int{0, 1, 4, 7, 8}
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

func CreateByYearFilterCallbackWithOutput(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
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
					outChan <- "EOF"
					continue
				}
				outBuilder.Reset()

				transactions := splitBatchInRows(body)
				for _, transaction := range transactions {
					if filtered, ok := filterAndExtractFields(transaction, minYearAllowed, maxYearAllowed, transactionFieldIndices); ok {
						outBuilder.WriteString(filtered)
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
