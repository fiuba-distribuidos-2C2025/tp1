package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
)

func concatTop3WithBirthdates(user string, neededUsers map[string]string) (string, bool) {
	// user_id,gender,birthdate,registered_at
	// 2018392,male,1992-12-27,2025-06-01 09:56:51
	fields := strings.Split(user, ",")
	if len(fields) < 4 {
		log.Errorf("Invalid user format: %s", user)
		return "", false
	}
	userId := fields[0] + ".0" // TODO: datasets show userId in different formats
	birthdate := fields[2]
	var sb strings.Builder
	if _, needed := neededUsers[userId]; needed {
		top3line := neededUsers[userId]
		elements := strings.Split(top3line, ",")
		if len(elements) < 3 {
			return "", false
		}
		// storeId,userId,PurchaseQty:
		// 1,1,10
		storeId := elements[0]
		// userId := elements[1]
		purchaseQty := elements[2]
		sb.WriteString(storeId)
		sb.WriteByte(',')
		sb.WriteString(birthdate)
		sb.WriteByte(',')
		sb.WriteString(purchaseQty)
	} else {
		return "", false
	}
	return sb.String(), true
}

// example store ids data input
// storeId,userId,PurchaseQty:
// 1,1,10
func processNeededUsers(top3Lines []string) map[string]string {
	// Preprocess stores data into a map for quick lookup
	neededUsers := make(map[string]string)
	for _, line := range top3Lines {
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			log.Errorf("Invalid top 3 data format: %s", line)
			continue
		}
		userId := fields[1]
		neededUsers[userId] = line
	}
	return neededUsers
}

func CreateByUserIdJoinerCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, top3PerStoreRowsChan chan string, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return CreatePrimaryQueueCallbackWithOutput(outChan, top3PerStoreRowsChan, processNeededUsers, neededEof, baseDir, workerID, messageSentNotificationChan, concatTop3WithBirthdates)
}
