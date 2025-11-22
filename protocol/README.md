# Client-Server Communication Protocol

## Overview

This document describes the asynchronous communication protocol used between the client and request_handler for CSV data transfer and result retrieval. The protocol uses a type-length-value (TLV) framing approach to prevent short reads/writes.

### Protocol Flow

The protocol supports an asynchronous request-response pattern where data upload and result retrieval occur in separate connections:

1. **Query Request Phase**: Client initiates a query, uploads data, and receives a query ID
2. **Processing Phase**: Server processes the data asynchronously
3. **Results Retrieval Phase**: Client reconnects using the query ID to retrieve results

This design allows clients to disconnect after uploading data and reconnect later to check if results are ready.

## File Type System

The protocol uses an enum system for file types to ensure both client and server agree on data categorization:

```go
type FileType int

const (
    FileTypeTransactions      FileType = 0  // Transaction data
    FileTypeTransactionItems  FileType = 1  // Transaction item details
    FileTypeStores            FileType = 2  // Store information
    FileTypeMenuItems         FileType = 3  // Menu item catalog
    FileTypeUsers             FileType = 4  // User information
)
```

### Directory Structure

The client expects the following directory structure under `/data`:

```
/data/
├── transactions/         → FileType 0 (FileTypeTransactions)
│   ├── file1.csv
│   └── file2.csv
├── transaction_items/    → FileType 1 (FileTypeTransactionItems)
│   ├── file1.csv
│   └── file2.csv
├── stores/               → FileType 2 (FileTypeStores)
│   ├── file1.csv
│   └── file2.csv
└── menu_items/           → FileType 3 (FileTypeMenuItems)
    ├── file1.csv
    └── file2.csv
└── users/                → FileType 4 (FileTypeUsers)
    ├── file1.csv
    └── file2.csv
```

## Message Types

All messages begin with a single byte message type identifier:

| Message Type | Value | Direction | Description |
|--------------|-------|-----------|-------------|
| `MessageTypeBatch` | 0x01 | Client → Server | CSV data batch |
| `MessageTypeEOF` | 0x02 | Client → Server | End of file type |
| `MessageTypeFinalEOF` | 0x03 | Client → Server | All data transferred |
| `MessageTypeACK` | 0x04 | Server → Client | Acknowledgment |
| `MessageTypeResultChunk` | 0x05 | Server → Client | Result data chunk |
| `MessageTypeResultEOF` | 0x06 | Server → Client | End of result |
| `MessageTypeQueryId` | 0x07 | Server → Client | Query ID for tracking |
| `MessageTypeQueryRequest` | 0x08 | Client → Server | Initiate new query |
| `MessageTypeResultsRequest` | 0x09 | Client → Server | Request results by query ID |
| `MessageTypeResultsPending` | 0x0A | Server → Client | Results not ready yet |
| `MessageTypeResultsReady` | 0x0B | Server → Client | Results ready to send |

## Communication Phases

### Phase 1: Query Initiation and Data Upload

1. Client connects to server
2. Client sends `MessageTypeQueryRequest` (0x08)
3. Client sends CSV data batches using `MessageTypeBatch` (0x01)
4. Client signals end of each file type with `MessageTypeEOF` (0x02)
5. Client signals completion with `MessageTypeFinalEOF` (0x03)
6. Server responds with `MessageTypeQueryId` (0x07) containing a unique query identifier
7. Client closes connection

### Phase 2: Results Retrieval

The client tracks pending results (queries 1-4) and retrieves them incrementally:

1. Client reconnects to server (can be done immediately or after a delay)
2. Client sends `MessageTypeResultsRequest` (0x09) with query ID
3. Server responds with either:
   - `MessageTypeResultsPending` (0x0A) - results not ready, client should retry later
   - `MessageTypeResultsReady` (0x0B) - results ready, followed by result chunks
4. If results are ready, server sends result chunks using `MessageTypeResultChunk` (0x05)
5. Server signals end of each result queue with `MessageTypeResultEOF` (0x06)
6. Client removes completed queries from its pending results list
7. Steps 1-6 repeat until all pending results are received (pendingResults list is empty)

## Message Formats

### 1. Query Request Message (0x08)

Initiates a new query session. No additional data.

**Format:**
```
┌─────────┐
│ Type    │
│ (1 byte)│
└─────────┘
```

### 2. Batch Message (0x01)

Transfers a batch of CSV data with metadata.

**Format:**
```
┌─────────┬──────────┬──────────┬──────────┬─────────────┬─────────────┬──────────┬─────────┐
│ Type    │ ClientID │ Frame    │ FileType │ CurrentChunk│ TotalChunks │ RowCount │ Payload │
│ (1 byte)│ (8 bytes)│ (4 bytes)│ (1 byte) │ (4 bytes)   │ (4 bytes)   │ (4 bytes)│(variable│
└─────────┴──────────┴──────────┴──────────┴─────────────┴─────────────┴──────────┴─────────┘
```

**Fields:**
- `Type`: 0x01
- `ClientID`: Client identifier (8-byte string, null-padded)
- `Frame Size`: Total size of frame (excluding type byte) - Big Endian uint32
- `FileType`: File type enum value (0-4) - 1 byte
- `CurrentChunk`: Current chunk number (1-indexed) - Big Endian uint32
- `TotalChunks`: Total number of chunks - Big Endian uint32
- `RowCount`: Number of CSV rows in this chunk - Big Endian uint32
- `Payload`: CSV rows separated by newlines - variable length

**Note**: ClientID changed from uint16 (2 bytes) to [8]byte string to support more flexible client identification.

### 3. EOF Message (0x02)

Signals completion of all files for a specific file type.

**Format:**
```
┌─────────┬──────────┐
│ Type    │ FileType │
│ (1 byte)│ (1 byte) │
└─────────┴──────────┘
```

**Fields:**
- `Type`: 0x02
- `FileType`: File type enum value (0-4)

### 4. Final EOF Message (0x03)

Signals all data transfer is complete for all file types.

**Format:**
```
┌─────────┐
│ Type    │
│ (1 byte)│
└─────────┘
```

### 5. Query ID Message (0x07)

Server sends query ID to client for tracking the request.

**Format:**
```
┌─────────┬─────────────┐
│ Type    │ QueryID     │
│ (1 byte)│ (8 bytes)   │
└─────────┴─────────────┘
```

**Fields:**
- `Type`: 0x07
- `QueryID`: Unique query identifier (8-byte string)

### 6. Results Request Message (0x09)

Client requests results for a specific query.

**Format:**
```
┌─────────┬─────────────┐
│ Type    │ QueryID     │
│ (1 byte)│ (8 bytes)   │
└─────────┴─────────────┘
```

**Fields:**
- `Type`: 0x09
- `QueryID`: Query identifier received in Phase 1 (8-byte string)

### 7. Results Pending Message (0x0A)

Server indicates results are not yet ready.

**Format:**
```
┌─────────┐
│ Type    │
│ (1 byte)│
└─────────┘
```

### 8. Results Ready Message (0x0B)

Server indicates results are ready and will begin sending them.

**Format:**
```
┌─────────┐
│ Type    │
│ (1 byte)│
└─────────┘
```

### 9. ACK Message (0x04)

Acknowledges successful receipt of a batch message.

**Format:**
```
┌─────────┐
│ Type    │
│ (1 byte)│
└─────────┘
```

### 10. Result Chunk Message (0x05)

Transfers a chunk of result data from the server to client.

**Format:**
```
┌─────────┬─────────┬─────────────┬─────────────┬─────────┬──────────┐
│ Type    │ QueueID │ CurrentChunk│ TotalChunks │ DataLen │ Data     │
│ (1 byte)│(4 bytes)│ (4 bytes)   │ (4 bytes)   │(4 bytes)│(variable)│
└─────────┴─────────┴─────────────┴─────────────┴─────────┴──────────┘
```

**Fields:**
- `Type`: 0x05
- `QueueID`: Result queue identifier (1-4) - Big Endian uint32
- `CurrentChunk`: Current chunk number - Big Endian uint32
- `TotalChunks`: Total chunks for this result - Big Endian uint32
- `DataLen`: Length of data - Big Endian uint32
- `Data`: Binary result data - variable length

### 11. Result EOF Message (0x06)

Signals end of results from a specific queue.

**Format:**
```
┌─────────┬─────────┐
│ Type    │ QueueID │
│ (1 byte)│(4 bytes)│
└─────────┴─────────┘
```

**Fields:**
- `Type`: 0x06
- `QueueID`: Result queue identifier - Big Endian uint32
