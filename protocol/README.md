# TODO: UPDATE PROTOCOL README

# Client-Server Communication Protocol

## Overview

This document describes the communication protocol used between the client and request_handler for CSV data transfer and result retrieval. The protocol uses a type-length-value (TLV) framing approach to prevent short reads/writes.

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

## Message Formats

### 1. Batch Message (0x01)

Transfers a batch of CSV data with metadata.

**Format:**
```
┌─────────┬──────────┬──────────┬─────────────┬─────────────┬──────────┬─────────┐
│ Type    │ Frame    │ FileType │ CurrentChunk│ TotalChunks │ RowCount │ Payload │
│ (1 byte)│ (4 bytes)│ (1 byte) │ (4 bytes)   │ (4 bytes)   │ (4 bytes)│(variable│
└─────────┴──────────┴──────────┴─────────────┴─────────────┴──────────┴─────────┘
```

**Fields:**
- `Type`: 0x01
- `Frame Size`: Total size of frame (excluding type byte) - Big Endian uint32
- `FileType`: File type enum value (0-3) - 1 byte
- `CurrentChunk`: Current chunk number (1-indexed) - Big Endian uint32
- `TotalChunks`: Total number of chunks - Big Endian uint32
- `RowCount`: Number of CSV rows in this chunk - Big Endian uint32
- `Payload`: CSV rows separated by newlines - variable length

### 2. EOF Message (0x02)

Signals completion of all files for a specific file type.

**Fields:**
- `Type`: 0x02
- `FileType`: File type enum value (0-3)

### 3. Final EOF Message (0x03)

Signals all data transfer is complete for all file types.

### 4. ACK Message (0x04)

Acknowledges successful receipt of a message.

### 5. Result Chunk Message (0x05)

Transfers a chunk of result data from the server to client.

**Fields:**
- `Type`: 0x05
- `QueueID`: Result queue identifier (1-4) - Big Endian uint32
- `CurrentChunk`: Current chunk number - Big Endian uint32
- `TotalChunks`: Total chunks for this result - Big Endian uint32
- `DataLen`: Length of data - Big Endian uint32
- `Data`: Binary result data - variable length

### 6. Result EOF Message (0x06)

Signals end of results from a specific queue.

**Fields:**
- `Type`: 0x06
- `QueueID`: Result queue identifier - Big Endian uint32
