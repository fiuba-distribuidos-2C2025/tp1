package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

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
				lines := strings.SplitN(payload, "\n", 3)

				// Separate header and the rest
				clientID := lines[0]
				// msgID := lines[1]
				items := lines[2]

				if items == "EOF" {
					if _, exists := clientEofCount[clientID]; !exists {
						clientEofCount[clientID] = 1
					} else {
						clientEofCount[clientID]++
					}

					eofCount := clientEofCount[clientID]
					if eofCount >= neededEof {
						outChan <- payload
						continue
					}
				}

				if items != "" {
					log.Info("SENDING THROUGH SECONDARY CHANNEL\n%s", items)
					outChan <- payload
				}
			}
		}
	}
}
