package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	"github.com/fiuba-distribuidos-2C2025/tp1/worker/common/utils"
)

// Validates if a transaction final amount is greater than the target amount.
// Sample transaction received:
// "TYPE,DATE,ITEM_ID,VALUE"
// "QUANTITY,2023-08,5,10"
func concatWithMenuItemsData(transaction string, menuItemsData map[string]string) (string, bool) {
	elements := strings.Split(transaction, ",")
	if len(elements) < 4 {
		return "", false
	}
	meassurement := elements[0]
	date := elements[1]
	itemId := elements[2]
	meassure := elements[3]

	menuItemName, exists := menuItemsData[itemId]
	if !exists {
		return "", false
	}

	var sb strings.Builder
	sb.WriteString(meassurement)
	sb.WriteByte(',')
	sb.WriteString(date)
	sb.WriteByte(',')
	sb.WriteString(menuItemName)
	sb.WriteByte(',')
	sb.WriteString(meassure)

	return sb.String(), true
}

// example menu items data input
// item_id,item_name,category,price,is_seasonal,available_from,available_to
// 2,Americano,coffee,7.0,False,,
func processMenuItems(menuItemLines []string) map[string]string {
	// Preprocess menu items data into a map for quick lookup
	menuItemsData := make(map[string]string)
	for _, line := range menuItemLines {
		fields := strings.Split(line, ",")
		if len(fields) < 6 {
			log.Errorf("Invalid menu item format: %s", line)
			continue
		}
		itemId := fields[0]
		menuItemsData[itemId] = fields[1]
	}
	return menuItemsData
}

func CreateByItemIdJoinerCallbackWithOutput(outChan chan string, messageSentNotificationChan chan string, neededEof int, menuItemRowsChan chan string, baseDir string, workerID string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		// Load existing clients EOF in case of worker restart
		clientEofs, err := utils.LoadClientsEofs(baseDir)
		if err != nil {
			log.Errorf("Error loading clients EOF count: %v", err)
			return
		}
		processedMenuItems := make(map[string]map[string]string)
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case secondaryQueueNewMessage, ok := <-menuItemRowsChan:
				if !ok {
					log.Errorf("menuItemRowsChan closed with error %v", err)
					return
				}

				log.Debugf("Received message from secondary queue being inside primary queue:\n%s", secondaryQueueNewMessage)
				payload := strings.TrimSpace(secondaryQueueNewMessage)
				rows := strings.Split(payload, "\n")
				if len(rows) < 2 {
					log.Errorf("Invalid secondary queue payload (need header + at least one row): %q", payload)
					continue
				}

				secondaryQueueClientID := rows[0]
				menuItemRows := rows[1:]
				processedMenu := processMenuItems(menuItemRows)
				processedMenuItems[secondaryQueueClientID] = processedMenu
				processPendingMessagesForClient(secondaryQueueClientID, processedMenuItems, clientEofs, outChan, messageSentNotificationChan, baseDir, workerID, neededEof, concatWithMenuItemsData)
			case msg, ok := <-*consumeChannel:
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				payload := strings.TrimSpace(string(msg.Body))
				lines := strings.Split(payload, "\n")

				// Separate header and the rest
				clientID := lines[0]
				msgID := lines[1]
				items := lines[2:]

				// Create accumulator for client
				if _, exists := processedMenuItems[clientID]; !exists {
					processedMenuItems[clientID] = make(map[string]string)
				}

				if len(processedMenuItems[clientID]) == 0 {
					log.Infof("Menu items data not yet available for client %s, storing message...", clientID)
					if items[0] == "EOF" {
						utils.StoreEOF(baseDir, clientID, msgID)
					} else {
						utils.StoreMessageWithChecksum(baseDir, clientID, msgID, payload)
					}
					// Acknowledge message
					msg.Ack(false)
					continue
				}

				if items[0] == "EOF" {
					// Store EOF on disk as tracking the count in memory is not secure
					// in case of worker restart
					utils.StoreEOF(baseDir, clientID, msgID)
					// Acknowledge message
					msg.Ack(false)
					if _, exists := clientEofs[clientID]; !exists {
						clientEofs[clientID] = make(map[string]string)
					}
					clientEofs[clientID][msgID] = ""

					eofs := clientEofs[clientID]
					eofCount := len(eofs)
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						// Use the workerID as msgID for the EOF
						// to ensure uniqueness across workers and restarts
						outChan <- clientID + "\n" + workerID + "\nEOF"
						// Here we just block until we are notified that the message was sent
						<-messageSentNotificationChan
						// clear accumulator memory
						delete(clientEofs, clientID)
						delete(processedMenuItems, clientID)
						utils.RemoveClientDir(baseDir, clientID)
						utils.RemoveClientDir(baseDir+"/secondary", clientID)
					}
					continue
				}

				// Reset builder for reuse
				outBuilder.Reset()

				for _, transaction := range items {
					if concatenated, ok := concatWithMenuItemsData(transaction, processedMenuItems[clientID]); ok {
						outBuilder.WriteString(concatenated)
						outBuilder.WriteByte('\n')
					}
				}

				if outBuilder.Len() > 0 {
					// Reuse the same msgID as it is already unique
					// and persistent across worker restarts
					outChan <- clientID + "\n" + msgID + "\n" + outBuilder.String()
					// Here we just block until we are notified that the message was sent
					<-messageSentNotificationChan
					log.Infof("Processed message")
				}
				// Acknowledge message
				msg.Ack(false)
				log.Infof("Processed message")
			}
		}
	}
}
