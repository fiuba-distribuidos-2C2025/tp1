package filterer

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// FilterFunc represents a function that filters and potentially transforms a transaction
type FilterFunc func(transaction string) (string, bool)

// Generic filter callback that handles the common message processing pattern
// All specific filters follow the same structure, only differing in the filter function used
func CreateGenericFilterCallbackWithOutput(outChan chan string, neededEof int, filterFunc FilterFunc) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		// Reusable buffer for building output
		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				log.Infof("Received message")
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}

				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]

				transactions := lines[1:]
				if transactions[0] == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					log.Debugf("Received eof (%d/%d)", eofCount, neededEof)
					if eofCount >= neededEof {
						msg := clientID + "\nEOF"
						outChan <- msg
					}
					continue
				}

				outBuilder.Reset()
				for _, transaction := range transactions {
					if filtered, ok := filterFunc(transaction); ok {
						outBuilder.WriteString(filtered + "\n")
					}
				}

				if outBuilder.Len() > 0 {
					outChan <- clientID + "\n" + outBuilder.String()
					log.Infof("Processed message")
				}
			}
		}
	}
}
