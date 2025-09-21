package common

// HEADER OF TRANSFER MSG
// TRANSFER;<FILE_TYPE>;<FILE_HASH>;<CURRENT_CHUNK>;<TOTAL_CHUNKS>
func SerializeCSVBatch(fileType int, fileHash string, totalChunks int64, currentChunk int64, rows [][]string) (string, error) {
	return "", nil
}
