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
	clientEofCount := map[string]int{}
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for secondary queue messages...")
		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("SECONDARY QUEUE MESSAGE RECEIVED")
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]
				items := lines[1:]

				if items[0] == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					if eofCount >= neededEof {
						outChan <- clientID + "\nEOF"
						continue
					}
				}

				// TODO: SPLITTING AND THEN JOINING BY SAME SEPARATOR
				// NOT GOOD.
				outMsg := strings.Join(items, "\n")
				if outMsg != "" {
					log.Info("SENDING THROUGH SECONDARY CHANNEL\n%s", outMsg)
					outChan <- clientID + "\n" + outMsg
				}
			}
		}
	}
}
