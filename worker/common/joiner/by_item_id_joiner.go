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

func CreateByItemIdJoinerCallbackWithOutput(outChan chan string, neededEof int, menuItemRowsChan chan string, baseDir string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	// Load existing clients EOF count in case of worker restart
	clientsEofCount, err := loadClientsEofCount(baseDir)
	if err != nil {
		log.Errorf("Error loading clients EOF count: %v", err)
		return nil
	}
	ResendClientEofs(clientsEofCount, neededEof, outChan, baseDir)
	processedMenuItems := make(map[string]map[string]string)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
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

				// Create accumulator for client
				if _, exists := processedMenuItems[clientID]; !exists {
					processedMenuItems[clientID] = make(map[string]string)
				}

				// Ensure we have menu items for this client.
				// This will block until the expected clientID arrives.
				// TODO: avoid blocking other clients while waiting for one
				for len(processedMenuItems[clientID]) == 0 {
					secondaryQueueNewMessage, ok := <-menuItemRowsChan
					if !ok {
						log.Errorf("menuItemRowsChan closed while waiting for client %s", clientID)
						return
					}

					log.Debugf("RECEIVED MSG FROM SECONDARY QUEUE, BEING INSIDE PRIMARY QUEUE:\n%s", secondaryQueueNewMessage)
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

					log.Warningf("Filled secondary queue data expected %s, got %s", clientID, secondaryQueueClientID)
				}

				items := lines[2:]
				if items[0] == "EOF" {
					// Store EOF on disk as tracking the count in memory is not secure
					// in case of worker restart
					utils.StoreEOF(baseDir, clientID, msgID)
					// Acknowledge message
					msg.Ack(false)
					if _, exists := clientsEofCount[clientID]; !exists {
						clientsEofCount[clientID] = 1
					} else {
						clientsEofCount[clientID]++
					}

					eofCount := clientsEofCount[clientID]
					log.Debugf("Received eof (%d/%d) from client %s", eofCount, neededEof, clientID)
					if eofCount >= neededEof {
						outChan <- clientID + "\nEOF"
						// clear accumulator memory
						delete(clientsEofCount, clientID)
						delete(processedMenuItems, clientID)
						utils.RemoveClientDir(baseDir, clientID)
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
					outChan <- clientID + "\n" + outBuilder.String()
				}
				// Acknowledge message
				msg.Ack(false)
				log.Infof("Processed message")
			}
		}
	}
}
