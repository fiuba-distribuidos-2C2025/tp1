package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Stores normal message (overwrites on same msgID).
func StoreMessage(baseDir string, clientID, msgID, body string) error {
	clientDir := filepath.Join(baseDir, clientID, "messages")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(clientDir, msgID)
	return os.WriteFile(path, []byte(body), 0o644)
}

// Stores EOF (overwrites on same msgID).
func StoreEOF(baseDir string, clientID, msgID string) error {
	eofDir := filepath.Join(baseDir, clientID, "eof")
	if err := os.MkdirAll(eofDir, 0o755); err != nil {
		return err
	}

	// One file per EOF message, keyed by msgID (idempotent on redelivery).
	path := filepath.Join(eofDir, msgID)
	if err := os.WriteFile(path, []byte("EOF\n"), 0o644); err != nil {
		return err
	}

	return nil
}

func ResendClientEofs(clientEofs map[string]map[string]string, neededEof int, outChan chan string, baseDir string, workerID string, messageSentNotificationChan chan string) {
	for clientID, eofs := range clientEofs {
		if len(eofs) >= neededEof {
			msgID := workerID
			outChan <- clientID + "\n" + msgID + "\nEOF"
			// Here we just block until we are notified that the message was sent
			<-messageSentNotificationChan
			RemoveClientDir(baseDir, clientID)
			delete(clientEofs, clientID)
		}
	}
}

// Stores normal message with checksum(overwrites on same msgID).
func StoreMessageWithChecksum(baseDir string, clientID, msgID, body string) error {
	clientDir := filepath.Join(baseDir, clientID, "messages")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(clientDir, msgID)
	checksum := len(body)
	var builder strings.Builder
	builder.WriteString(strconv.Itoa(checksum))
	builder.WriteString("\n")
	builder.WriteString(body)
	data := builder.String()
	return os.WriteFile(path, []byte(data), 0o644)
}

// Returns a map of msgID for all stored EOFs for a client.
func getEofs(baseDir, clientID string) (map[string]string, error) {
	eofDir := filepath.Join(baseDir, clientID, "eof")
	eofs := make(map[string]string)
	entries, err := os.ReadDir(eofDir)
	if err != nil {
		// If the directory does not exist, return empty map
		if os.IsNotExist(err) {
			return eofs, nil
		}
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		msgID := e.Name()
		eofs[msgID] = ""
	}
	return eofs, nil
}

// Returns the number of stored EOF messages for a client.
func GetEOFCount(baseDir, clientID string) (int, error) {
	eofDir := filepath.Join(baseDir, clientID, "eof")
	entries, err := os.ReadDir(eofDir)
	if err != nil {
		return 0, nil
	}
	return len(entries), nil
}

// Checks all client directories in baseDir to see if they have reached the EOF threshold.
func CheckAllClientsEOFThresholds(outChan chan string, baseDir string, neededEof int, workerID string, messageSentNotificationChan chan string, thresholdReachedHandle func(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error) error {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages yet, nothing to do.
			return nil
		}
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		clientID := e.Name()
		thresholdReached, err := ThresholdReached(baseDir, clientID, neededEof)
		if err != nil {
			return err
		}
		if thresholdReached {
			err := thresholdReachedHandle(outChan, messageSentNotificationChan, baseDir, clientID, workerID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Checks if a client has reached the EOF threshold.
func ThresholdReached(baseDir, clientID string, neededEof int) (bool, error) {
	count, err := GetEOFCount(baseDir, clientID)
	if err != nil {
		return false, err
	}

	return count >= neededEof, nil
}

// RemoveClientDir deletes the entire directory for a given clientID.
func RemoveClientDir(baseDir string, clientID string) error {
	clientDir := filepath.Join(baseDir, clientID)

	if err := os.RemoveAll(clientDir); err != nil {
		return fmt.Errorf("failed to remove %s: %w", clientDir, err)
	}

	return nil
}

func LoadClientsEofs(baseDir string) (map[string]map[string]string, error) {
	clientEofs := make(map[string]map[string]string)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages yet, nothing to do.
			return clientEofs, nil
		}
		return clientEofs, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		clientID := e.Name()
		eofs, err := getEofs(baseDir, clientID)
		if err != nil {
			return clientEofs, err
		}
		clientEofs[clientID] = eofs

	}
	return clientEofs, nil
}

func LoadClientMessagesWithChecksums(baseDir, clientID string) (map[string]string, error) {
	messagesDir := filepath.Join(baseDir, clientID, "messages")
	messageEntries, err := os.ReadDir(messagesDir)
	if err != nil {
		return nil, err
	}
	clientMessages := make(map[string]string)

	for _, me := range messageEntries {
		if me.IsDir() {
			continue
		}
		msgID := me.Name()
		data, err := os.ReadFile(filepath.Join(messagesDir, msgID))
		if err != nil {
			continue
		}
		split := strings.SplitN(string(data), "\n", 2)
		checksum, err := strconv.Atoi(split[0])
		if err != nil {
			// We just ignore messages with invalid checksum
			// They will be resent by the queue
			continue
		}
		body := split[1]
		if checksum != len(body) {
			// We just ignore messages with invalid checksum
			// They will be resent by the queue
			continue
		}
		clientMessages[msgID] = body
	}
	return clientMessages, nil
}

func LoadAllClientsMessagesWithChecksums(baseDir string) (map[string]map[string]string, error) {
	allClientMessages := make(map[string]map[string]string)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages yet, nothing to do.
			return allClientMessages, nil
		}
		return allClientMessages, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		clientID := e.Name()
		clientMessages, err := loadClientMessagesWithChecksums(baseDir, clientID, allClientMessages)
		if err != nil {
			return nil, err
		}
		allClientMessages[clientID] = clientMessages

	}
	return allClientMessages, nil
}

func eofAlreadyExists(baseDir string, clientID string, msgID string) (bool, error) {
	eofPath := filepath.Join(baseDir, clientID, "eof", msgID+".eof")
	if _, err := os.Stat(eofPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func MessageAlreadyExists(baseDir string, clientID string, msgID string) (bool, error) {
	exists, err := eofAlreadyExists(baseDir, clientID, msgID)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	messagePath := filepath.Join(baseDir, clientID, "messages", msgID+".txt")
	if _, err := os.Stat(messagePath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func SendEof(outChan chan string, messageSentNotificationChan chan string, baseDir string, clientID string, workerID string) error {
	// Use the workerID as msgID for the EOF
	// to ensure uniqueness across workers and restarts
	outChan <- clientID + "\n" + workerID + "\nEOF"
	// Here we just block until we are notified that the message was sent
	<-messageSentNotificationChan
	return RemoveClientDir(baseDir, clientID)
}
