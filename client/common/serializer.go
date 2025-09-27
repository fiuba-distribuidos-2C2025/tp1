package common

import (
	"fmt"
)

// HEADER OF TRANSFER MSG
// TRANSFER;<FILE_TYPE>;<FILE_HASH>;<CURRENT_CHUNK>;<TOTAL_CHUNKS>
func SerializeCSVBatch(fileType int, fileHash string, totalChunks int, currentChunk int, rows [][]string) string {
	header := fmt.Sprintf("TRANSFER;%d;%s;%d;%d\n", fileType, fileHash, currentChunk, totalChunks)
	body := ""
	for _, row := range rows {
		for j, field := range row {
			body += field
			if j < len(row)-1 {
				body += ","
			}
		}
		body += "\n"
	}

	return header + body

}
