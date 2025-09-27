package filter

import (
	"strconv"
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func CreateDummyFilterCallbackWithOutput(outChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
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
				n, parseErr := strconv.ParseInt(body, 10, 64)
				if parseErr != nil {
					log.Errorf("[worker] invalid integer %q: %v", body, parseErr)
					continue
				}

				log.Infof("Received number: %d", n)
				if n%2 == 0 {
					outChan <- strconv.FormatInt(n, 10)
				}
			}
		}
	}
}
