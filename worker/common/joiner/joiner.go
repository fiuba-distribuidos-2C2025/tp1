package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Splits a message into an array of strings
// using the newline separator.
func splitBatchInRows(body string) []string {
	return strings.Split(body, "\n")
}

// Removes unnecessary fields from a csv string
// keeping only the specified indices.
func removeNeedlessFields(row string, indices []int) string {
	elements := strings.Split(row, ",")
	needed := make([]string, 0, len(indices))

	for _, idx := range indices {
		if idx >= 0 && idx < len(elements) {
			needed = append(needed, elements[idx])
		}
	}

	return strings.Join(needed, ",")
}

func CreateSecondQueueCallbackWithOutput(outChan chan string, neededEof int) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")
		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("MESSAGE RECEIVED")
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				body := strings.TrimSpace(string(msg.Body))
				log.Infof("received", body)

				if body == "EOF" {
					eofCount++
					if eofCount >= neededEof {
						outChan <- "EOF"
						continue
					}
				}

				outChan <- body
			}
		}
	}
}
