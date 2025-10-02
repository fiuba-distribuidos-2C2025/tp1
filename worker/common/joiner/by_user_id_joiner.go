package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

func concatTop3(top3PerStoreArray []string, usersBirthdays map[string]string) string {
	var sb strings.Builder

	for _, line := range top3PerStoreArray {
		elements := strings.Split(line, ",")
		if len(elements) < 3 {
			continue
		}
		// storeId,userId,PurchaseQty:
		// 1,1,10
		storeId := elements[0]
		userId := elements[1]
		purchaseQty := elements[2]

		birthdate, exists := usersBirthdays[userId]
		if !exists {
			birthdate = "UNKNOWN"
		}

		sb.WriteString(storeId)
		sb.WriteByte(',')
		sb.WriteString(birthdate)
		sb.WriteByte(',')
		sb.WriteString(purchaseQty)
		sb.WriteByte('\n')
	}

	return sb.String()
}

// example store ids data input
// storeId,userId,PurchaseQty:
// 1,1,10
func processNeededUsers(top3Rows string) (map[string]string, []string) {
	// Preprocess stores data into a map for quick lookup
	neededUsers := make(map[string]string)
	top3Lines := splitBatchInRows(top3Rows)
	for _, line := range top3Lines {
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			log.Errorf("Invalid top 3 data format: %s", line)
			continue
		}
		userId := fields[1]
		neededUsers[userId] = ""
	}
	return neededUsers, top3Lines
}

func CreateByUserIdJoinerCallbackWithOutput(outChan chan string, neededEof int, top3PerStoreRows string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	neededUsers, top3PerStoreArray := processNeededUsers(top3PerStoreRows)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

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

				if body == "EOF" {
					eofCount++
					if eofCount >= neededEof {
						outChan <- concatTop3(top3PerStoreArray, neededUsers)
						outChan <- "EOF"
						continue
					}
				}

				// Reset builder for reuse
				users := splitBatchInRows(body)
				outBuilder.Reset()

				for _, user := range users {
					// user_id,gender,birthdate,registered_at
					fields := strings.Split(user, ",")
					if len(fields) < 4 {
						log.Errorf("Invalid user format: %s", user)
						continue
					}
					userId := fields[0]
					birthdate := fields[2]
					if _, needed := neededUsers[userId]; needed {
						neededUsers[userId] = birthdate
					}
				}

				if outBuilder.Len() > 0 {
					outChan <- outBuilder.String()
					log.Infof("Processed message")
				}
			}
		}
	}
}
