package common

import (
	"fmt"
	"strings"
)

// SerializeCSVBatch creates the message format expected by the request handler
// Format: TRANSFER;<FILE_TYPE>;<FILE_HASH>;<CURRENT_CHUNK>;<TOTAL_CHUNKS>\n
//
//	csv_row0\n
//	csv_row1\n
//	...\n
func SerializeCSVBatch(fileType int, fileHash string, totalChunks, currentChunk int, rows [][]string) string {
	var sb strings.Builder

	// Write header
	sb.WriteString(fmt.Sprintf("TRANSFER;%d;%s;%d;%d\n", fileType, fileHash, currentChunk, totalChunks))

	// Write CSV rows (joining fields with commas)
	for _, row := range rows {
		sb.WriteString(strings.Join(row, ","))
		sb.WriteString("\n")
	}

	return sb.String()
}
