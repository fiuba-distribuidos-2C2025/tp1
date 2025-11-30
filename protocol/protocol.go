package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// FileType represents the type of data being transferred
type FileType int

const (
	FileTypeTransactions FileType = iota
	FileTypeTransactionItems
	FileTypeStores
	FileTypeMenuItems
	FileTypeUsers
)

// QueueName returns the queue name prefix for this file type
func (ft FileType) QueueName() string {
	switch ft {
	case FileTypeTransactions:
		return "transactions"
	case FileTypeTransactionItems:
		return "transactions_items"
	case FileTypeStores:
		return "stores"
	case FileTypeMenuItems:
		return "menu_items"
	case FileTypeUsers:
		return "users"
	default:
		return ""
	}
}

// IsValid checks if the file type is valid
func (ft FileType) IsValid() bool {
	return ft >= FileTypeTransactions && ft <= FileTypeUsers
}

// MessageType represents the type of message being sent
type MessageType byte

const (
	MessageTypeBatch          MessageType = 0x01
	MessageTypeEOF            MessageType = 0x02
	MessageTypeFinalEOF       MessageType = 0x03
	MessageTypeACK            MessageType = 0x04
	MessageTypeResultChunk    MessageType = 0x05
	MessageTypeResultEOF      MessageType = 0x06
	MessageTypeQueryId        MessageType = 0x07
	MessageTypeQueryRequest   MessageType = 0x08
	MessageTypeResultsRequest MessageType = 0x09
	MessageTypeResultsPending MessageType = 0x0A
	MessageTypeResultsReady   MessageType = 0x0B
	MessageTypeResumeRequest  MessageType = 0x0C
	MessageTypeHealthCheck    MessageType = 0x0D
)

// BatchMessage represents a data batch being transferred
type BatchMessage struct {
	ClientID     [8]byte
	FileType     FileType
	CurrentChunk int32
	TotalChunks  int32
	CSVRows      []string
}

func ClientIDFromString(s string) ([8]byte, error) {
	var clientID [8]byte
	idBytes := []byte(s)
	if len(idBytes) > 8 {
		return clientID, fmt.Errorf("client ID too long: max 8 bytes, got %d", len(idBytes))
	}
	copy(clientID[:], idBytes)
	return clientID, nil
}

func ClientIDToString(clientID [8]byte) string {
	s := string(clientID[:])
	// Trim trailing null bytes
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return s[:i]
		}
	}
	return s
}

// EOFMessage represents an end-of-file marker for a specific file type
type EOFMessage struct {
	FileType FileType
}

// ResumeRequestMessage indicates reconnection with existing queryID
type ResumeRequestMessage struct {
	QueryID string
}

// ResultChunkMessage represents a chunk of result data
type ResultChunkMessage struct {
	QueueID      int32
	CurrentChunk int32
	TotalChunks  int32
	Data         []byte
}

// ResultEOFMessage represents the end of results from a specific queue
type ResultEOFMessage struct {
	QueueID int32
}

// QueryIdMessage represents the identifier generated for an specific request
type QueryIdMessage struct {
	QueryID string
}

// ResultRequestMessage represents the identifier generated for an specific request
type ResultRequestMessage struct {
	QueryID          string
	RequestedResults []int
}

// Protocol handles message serialization and deserialization with proper framing
// This version uses buffered I/O to prevent short reads/writes
type Protocol struct {
	writer *bufio.Writer
	reader *bufio.Reader
}

// NewProtocol creates a new Protocol instance with buffered I/O
func NewProtocol(rw io.ReadWriter) *Protocol {
	return &Protocol{
		writer: bufio.NewWriterSize(rw, 16*1024*1024), // 16MB write buffer
		reader: bufio.NewReaderSize(rw, 16*1024*1024), // 16MB read buffer
	}
}

// writeFull ensures all data is written and flushed, handling short writes
func (p *Protocol) writeFull(data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := p.writer.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("write failed after %d/%d bytes: %w", totalWritten, len(data), err)
		}
		if n == 0 {
			return fmt.Errorf("write returned 0 bytes without error at %d/%d", totalWritten, len(data))
		}
		totalWritten += n
	}

	// Flush to ensure data reaches the network
	if err := p.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}

// SendBatch sends a batch message with proper framing and short-write protection
func (p *Protocol) SendBatch(msg *BatchMessage) error {
	// Build complete message in memory first
	var buf bytes.Buffer

	// Build payload (CSV rows joined with newlines)
	var payload bytes.Buffer
	for i, row := range msg.CSVRows {
		payload.WriteString(row)
		if i < len(msg.CSVRows)-1 {
			payload.WriteByte('\n')
		}
	}
	payloadBytes := payload.Bytes()

	// Write message type
	buf.WriteByte(byte(MessageTypeBatch))

	// Calculate frame size: clientID(8) + fileType(1) + currentChunk(4) + totalChunks(4) + rowCount(4) + payload
	frameSize := 8 + 1 + 4 + 4 + 4 + len(payloadBytes)

	// Write frame header (25 bytes)
	header := make([]byte, 25)
	copy(header[0:8], msg.ClientID[:])
	binary.BigEndian.PutUint32(header[8:12], uint32(frameSize))
	header[12] = byte(msg.FileType)
	binary.BigEndian.PutUint32(header[13:17], uint32(msg.CurrentChunk))
	binary.BigEndian.PutUint32(header[17:21], uint32(msg.TotalChunks))
	binary.BigEndian.PutUint32(header[21:25], uint32(len(msg.CSVRows)))
	buf.Write(header)

	// Write payload
	buf.Write(payloadBytes)

	// Write entire message atomically with short-write protection and flush
	return p.writeFull(buf.Bytes())
}

// ReceiveBatch receives a batch message with short-read protection
func (p *Protocol) ReceiveBatch() (*BatchMessage, error) {
	// Read frame header (25 bytes) - io.ReadFull handles short reads from bufio.Reader
	header := make([]byte, 25)
	if _, err := io.ReadFull(p.reader, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var clientID [8]byte
	copy(clientID[:], header[0:8])
	frameSize := binary.BigEndian.Uint32(header[8:12])
	fileType := FileType(header[12])
	currentChunk := int32(binary.BigEndian.Uint32(header[13:17]))
	totalChunks := int32(binary.BigEndian.Uint32(header[17:21]))
	rowCount := binary.BigEndian.Uint32(header[21:25])

	if !fileType.IsValid() {
		return nil, fmt.Errorf("invalid file type: %d", fileType)
	}

	// Validate frame size
	expectedMinSize := 8 + 1 + 4 + 4 + 4
	if frameSize < uint32(expectedMinSize) {
		return nil, fmt.Errorf("invalid frame size: %d (expected at least %d)", frameSize, expectedMinSize)
	}

	// Calculate and read payload - io.ReadFull handles short reads
	payloadSize := frameSize - (8 + 1 + 4 + 4 + 4)
	payload := make([]byte, payloadSize)
	if payloadSize > 0 {
		if _, err := io.ReadFull(p.reader, payload); err != nil {
			return nil, fmt.Errorf("failed to read payload (size=%d): %w", payloadSize, err)
		}
	}

	// Parse CSV rows from payload
	rows := make([]string, 0, rowCount)
	if payloadSize > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(payload))
		for scanner.Scan() {
			rows = append(rows, scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("failed to parse CSV rows: %w", err)
		}
	}

	return &BatchMessage{
		ClientID:     clientID,
		FileType:     fileType,
		CurrentChunk: currentChunk,
		TotalChunks:  totalChunks,
		CSVRows:      rows,
	}, nil
}

// SendEOF sends an EOF message for a specific file type
func (p *Protocol) SendEOF(fileType FileType) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeEOF))
	buf.WriteByte(byte(fileType))
	return p.writeFull(buf.Bytes())
}

// ReceiveEOF receives an EOF message
func (p *Protocol) ReceiveEOF() (*EOFMessage, error) {
	data := make([]byte, 1)
	if _, err := io.ReadFull(p.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read file type: %w", err)
	}

	fileType := FileType(data[0])
	if !fileType.IsValid() {
		return nil, fmt.Errorf("invalid file type: %d", fileType)
	}

	return &EOFMessage{FileType: fileType}, nil
}

// SendFinalEOF sends the final EOF marker
func (p *Protocol) SendFinalEOF() error {
	return p.writeFull([]byte{byte(MessageTypeFinalEOF)})
}

// SendACK sends an acknowledgment
func (p *Protocol) SendACK() error {
	return p.writeFull([]byte{byte(MessageTypeACK)})
}

// ReceiveACK receives an acknowledgment
func (p *Protocol) ReceiveACK() error {
	// ACK has no additional data, message type already read
	return nil
}

// SendResultChunk sends a result data chunk
func (p *Protocol) SendResultChunk(msg *ResultChunkMessage) error {
	var buf bytes.Buffer

	buf.WriteByte(byte(MessageTypeResultChunk))

	header := make([]byte, 16)
	binary.BigEndian.PutUint32(header[0:4], uint32(msg.QueueID))
	binary.BigEndian.PutUint32(header[4:8], uint32(msg.CurrentChunk))
	binary.BigEndian.PutUint32(header[8:12], uint32(msg.TotalChunks))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(msg.Data)))
	buf.Write(header)

	buf.Write(msg.Data)

	return p.writeFull(buf.Bytes())
}

// ReceiveResultChunk receives a result data chunk
func (p *Protocol) ReceiveResultChunk() (*ResultChunkMessage, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(p.reader, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	queueID := int32(binary.BigEndian.Uint32(header[0:4]))
	currentChunk := int32(binary.BigEndian.Uint32(header[4:8]))
	totalChunks := int32(binary.BigEndian.Uint32(header[8:12]))
	dataLen := binary.BigEndian.Uint32(header[12:16])

	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(p.reader, data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
	}

	return &ResultChunkMessage{
		QueueID:      queueID,
		CurrentChunk: currentChunk,
		TotalChunks:  totalChunks,
		Data:         data,
	}, nil
}

// SendResultEOF sends the result EOF marker
func (p *Protocol) SendResultEOF(queueID int32) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeResultEOF))

	queueIDBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(queueIDBuf, uint32(queueID))
	buf.Write(queueIDBuf)

	return p.writeFull(buf.Bytes())
}

// SendQueryId sends the query ID generated for the request
func (p *Protocol) SendQueryId(queryID string) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeQueryId))
	buf.WriteString(queryID)

	return p.writeFull(buf.Bytes())
}

// SendQueryRequest sends the query request marker
func (p *Protocol) SendQueryRequest() error {
	return p.writeFull([]byte{byte(MessageTypeQueryRequest)})
}

// SendResultsRequest sends the query ID for the needed response
// and the specific results it wants to retrieve
func (p *Protocol) SendResultsRequest(queryID string, resultIDs []int) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeResultsRequest))
	buf.WriteString(queryID)
	binary.Write(&buf, binary.BigEndian, int32(len(resultIDs)))
	for _, id := range resultIDs {
		binary.Write(&buf, binary.BigEndian, int32(id))
	}

	return p.writeFull(buf.Bytes())
}

// SendResultsPending notifies that results are not ready yet
func (p *Protocol) SendResultsPending() error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeResultsPending))

	return p.writeFull(buf.Bytes())
}

// SendResultsReady notifies that results are ready to be sent
func (p *Protocol) SendResultsReady() error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeResultsReady))

	return p.writeFull(buf.Bytes())
}

// ReceiveResultEOF receives the result EOF marker
func (p *Protocol) ReceiveResultEOF() (*ResultEOFMessage, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return nil, fmt.Errorf("failed to read queue ID: %w", err)
	}

	queueID := int32(binary.BigEndian.Uint32(buf))

	return &ResultEOFMessage{QueueID: queueID}, nil
}

// ReceiveQueryId receives the query ID generated for the request
func (p *Protocol) ReceiveQueryId() (*QueryIdMessage, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return nil, fmt.Errorf("failed to read query ID: %w", err)
	}

	queryID := string(buf)

	return &QueryIdMessage{QueryID: queryID}, nil
}

// ReceiveResultsRequest receives the query ID generated for the request
func (p *Protocol) ReceiveResultsRequest() (*ResultRequestMessage, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return nil, fmt.Errorf("failed to read query ID: %w", err)
	}
	queryID := string(buf)

	var count int32
	if err := binary.Read(p.reader, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read result count: %w", err)
	}

	requestedResults := make([]int, int(count))
	for i := range requestedResults {
		var v int32
		if err := binary.Read(p.reader, binary.BigEndian, &v); err != nil {
			return nil, fmt.Errorf("failed to read result ID: %w", err)
		}
		requestedResults[i] = int(v)
	}

	return &ResultRequestMessage{
		QueryID:          queryID,
		RequestedResults: requestedResults,
	}, nil
}

// ReceiveMessage receives any message type and returns the type and appropriate data
func (p *Protocol) ReceiveMessage() (MessageType, interface{}, error) {
	msgTypeBuf := make([]byte, 1)
	if _, err := io.ReadFull(p.reader, msgTypeBuf); err != nil {
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}

	msgType := MessageType(msgTypeBuf[0])

	switch msgType {
	case MessageTypeBatch:
		msg, err := p.ReceiveBatch()
		return msgType, msg, err

	case MessageTypeEOF:
		msg, err := p.ReceiveEOF()
		return msgType, msg, err

	case MessageTypeFinalEOF:
		return msgType, nil, nil

	case MessageTypeACK:
		return msgType, nil, p.ReceiveACK()

	case MessageTypeResultChunk:
		msg, err := p.ReceiveResultChunk()
		return msgType, msg, err

	case MessageTypeResultEOF:
		msg, err := p.ReceiveResultEOF()
		return msgType, msg, err

	case MessageTypeQueryId:
		msg, err := p.ReceiveQueryId()
		return msgType, msg, err

	case MessageTypeQueryRequest:
		return msgType, nil, nil

	case MessageTypeResultsRequest:
		msg, err := p.ReceiveResultsRequest()
		return msgType, msg, err

	case MessageTypeResultsPending:
		return msgType, nil, nil

	case MessageTypeResultsReady:
		return msgType, nil, nil

	case MessageTypeResumeRequest:
		msg, err := p.ReceiveResumeRequest()
		return msgType, msg, err

	case MessageTypeHealthCheck:
		return msgType, nil, nil

	default:
		return 0, nil, fmt.Errorf("unknown message type: 0x%02x", msgType)
	}
}

// SerializeCSVBatch is a legacy helper function for backward compatibility
// Deprecated: Use SendBatch instead
func SerializeCSVBatch(fileType int, fileHash string, totalChunks, currentChunk int, rows [][]string) string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("TRANSFER;%d;%s;%d;%d\n",
		fileType, fileHash, currentChunk, totalChunks))

	for _, row := range rows {
		builder.WriteString(strings.Join(row, ","))
		builder.WriteString("\n")
	}

	builder.WriteString("\n")
	return builder.String()
}

func (p *Protocol) SendResumeRequest(queryID string) error {
	var buf bytes.Buffer
	buf.WriteByte(byte(MessageTypeResumeRequest))
	buf.WriteString(queryID)
	return p.writeFull(buf.Bytes())
}

func (p *Protocol) ReceiveResumeRequest() (*ResumeRequestMessage, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return nil, fmt.Errorf("failed to read query ID: %w", err)
	}

	// Trim null bytes just like in ClientIDToString
	queryID := string(buf)
	for i := 0; i < len(queryID); i++ {
		if queryID[i] == 0 {
			queryID = queryID[:i]
			break
		}
	}

	return &ResumeRequestMessage{QueryID: queryID}, nil
}

func (p *Protocol) SendHealthCheck() error {
	return p.writeFull([]byte{byte(MessageTypeHealthCheck)})
}

func (p *Protocol) SendHealthCheckResponse() error {
	return p.writeFull([]byte{byte(MessageTypeACK)})
}
