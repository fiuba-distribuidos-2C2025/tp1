package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// parseResultFileName extracts timestamp, clientID, and queueID from filename
// Expected format: final_results_<queueID>_<timestamp>_<clientID>.txt
func parseResultFileName(filename string) (time.Time, string, int, error) {
	basename := filepath.Base(filename)
	basename = strings.TrimSuffix(basename, ".txt")

	// Remove "final_results_" prefix
	if !strings.HasPrefix(basename, "final_results_") {
		return time.Time{}, "", 0, fmt.Errorf("invalid filename format")
	}
	basename = strings.TrimPrefix(basename, "final_results_")

	// Split by underscore: <queueID>_<timestamp>_<clientID>
	parts := strings.SplitN(basename, "_", 3)
	if len(parts) != 3 {
		return time.Time{}, "", 0, fmt.Errorf("expected 3 parts, got %d", len(parts))
	}

	queueID, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("invalid queue ID: %w", err)
	}

	timestampUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("invalid timestamp: %w", err)
	}
	timestamp := time.Unix(timestampUnix, 0)

	clientID := parts[2]

	return timestamp, clientID, queueID, nil
}

// createClientResultFiles creates placeholder files for all result queues
func createClientResultFiles(clientId string, timestamp time.Time) error {
	timestampUnix := timestamp.Unix()

	for queueID := 1; queueID <= 4; queueID++ {
		fileName := filepath.Join(OwnResultsDir, fmt.Sprintf("final_results_%d_%d_%s.txt", queueID, timestampUnix, clientId))

		// Create empty file or with a placeholder
		if err := os.WriteFile(fileName, []byte{}, 0644); err != nil {
			return fmt.Errorf("failed to create file %s: %w", fileName, err)
		}

		log.Infof("Created result file: %s", fileName)
	}

	return nil
}

// findResultFile searches for a result file in both directories
func findResultFile(queueID int, queryId string) string {
	// Check own results first
	ownPattern := filepath.Join(OwnResultsDir, fmt.Sprintf("final_results_%d_*_%s.txt", queueID, queryId))
	ownMatches, _ := filepath.Glob(ownPattern)
	if len(ownMatches) > 0 {
		return ownMatches[0]
	}

	// Check external results
	externalPattern := filepath.Join(ExternalResultsDir, fmt.Sprintf("final_results_%d_*_%s.txt", queueID, queryId))
	externalMatches, _ := filepath.Glob(externalPattern)
	if len(externalMatches) > 0 {
		return externalMatches[0]
	}

	return ""
}
