package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

// Stores normal message (overwrites on same msgID).
func StoreMessage(baseDir string, clientID, msgID, body string) error {
	clientDir := filepath.Join(baseDir, clientID, "messages")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(clientDir, msgID+".txt")
	return os.WriteFile(path, []byte(body), 0o644)
}

// Stores EOF (overwrites on same msgID).
func StoreEOF(baseDir string, clientID, msgID string) error {
	eofDir := filepath.Join(baseDir, clientID, "eof")
	if err := os.MkdirAll(eofDir, 0o755); err != nil {
		return err
	}

	// One file per EOF message, keyed by msgID (idempotent on redelivery).
	path := filepath.Join(eofDir, msgID+".eof")
	if err := os.WriteFile(path, []byte("EOF\n"), 0o644); err != nil {
		return err
	}

	return nil
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

func LoadClientsEofCount(baseDir string) (map[string]int, error) {
	clientsEofCount := make(map[string]int)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No messages yet, nothing to do.
			return clientsEofCount, nil
		}
		return clientsEofCount, err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		clientID := e.Name()
		eofCount, err := GetEOFCount(baseDir, clientID)
		if err != nil {
			return clientsEofCount, err
		}
		clientsEofCount[clientID] = eofCount

	}
	return clientsEofCount, nil
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
