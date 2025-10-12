package joiner

import (
	"strings"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
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
func processMenuItems(menuItemRows string) map[string]string {
	// Preprocess menu items data into a map for quick lookup
	menuItemsData := make(map[string]string)
	menuItemLines := splitBatchInRows(menuItemRows)
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

func CreateByItemIdJoinerCallbackWithOutput(outChan chan string, neededEof int, menuItemRowsChan chan string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	clientEofCount := map[string]int{}
	processedMenuItems := make(map[string]map[string]string)
	// processedMenuItems := processMenuItems(menuItemRows)
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

				// Create accumulator for client
				if _, exists := processedMenuItems[clientID]; !exists {
					processedMenuItems[clientID] = make(map[string]string)
				}

				// Check if we have the needed secondary queue data
				if len(processedMenuItems[clientID]) == 0 {
					// This means we still haven't received any menu items for this client
					select {
					// TODO: CONSUME IN RANGE INSTEAD OF ONLY ONCE
					case secondaryQueueNewMessage := <-menuItemRowsChan:
						log.Debugf("RECEIVED MSG FROM SECONDARY QUEUE, BEING INSIDE PRIMARY QUEUE:\n%s", secondaryQueueNewMessage)
						payload := strings.TrimSpace(secondaryQueueNewMessage)
						lines := strings.Split(payload, "\n")
						secondaryQueueClientID := lines[0]
						menuItemRows := lines[1]
						processedMenu := processMenuItems(menuItemRows)
						processedMenuItems[secondaryQueueClientID] = processedMenu
						if clientID != secondaryQueueClientID {
							log.Warningf("Filled secondary queue data, but for a different client, expected %s, got %s", clientID, secondaryQueueClientID)
							continue
						}

					default:
						log.Debugf("Received primary queue message, but secondary queue is still blocking")
						continue
					}
				}

				// Ack message only if we have secondary queue data to handle it
				msg.Ack(false)

				items := lines[1:]
				if items[0] == "EOF" {
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

				// Reset builder for reuse
				outBuilder.Reset()

				outBuilder.WriteString(clientID + "\n")
				for _, transaction := range items {
					if concatenated, ok := concatWithMenuItemsData(transaction, processedMenuItems[clientID]); ok {
						outBuilder.WriteString(concatenated)
						outBuilder.WriteByte('\n')
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
