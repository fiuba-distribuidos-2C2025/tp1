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
	sb.WriteString(meassure)
	sb.WriteByte(',')
	sb.WriteString(menuItemName)

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

func CreateByItemIdJoinerCallbackWithOutput(outChan chan string, neededEof int, menuItemRows string) func(consumeChannel middleware.ConsumeChannel, done chan error) {
	eofCount := 0
	processedMenuItems := processMenuItems(menuItemRows)
	return func(consumeChannel middleware.ConsumeChannel, done chan error) {
		log.Infof("Waiting for messages...")

		var outBuilder strings.Builder

		for {
			select {
			case msg, ok := <-*consumeChannel:
				msg.Ack(false)
				if !ok {
					log.Infof("Deliveries channel closed; shutting down")
					return
				}
				body := strings.TrimSpace(string(msg.Body))

				if body == "EOF" {
					eofCount++
					log.Debug("Received eof (%d/%d)", eofCount, neededEof)
					if eofCount >= neededEof {
						outChan <- "EOF"
					}
					continue
				}

				// Reset builder for reuse
				items := splitBatchInRows(body)
				outBuilder.Reset()

				for _, transaction := range items {
					if concatenated, ok := concatWithMenuItemsData(transaction, processedMenuItems); ok {
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
