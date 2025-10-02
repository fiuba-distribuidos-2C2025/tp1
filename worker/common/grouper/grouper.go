package grouper

import (
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Splits a message into an array of strings
// using the newline separator.
func splitBatchInRows(body string) []string {
	return strings.Split(body, "\n")
}

// Removes unnecessary fields from a csv string
// keeping only the specified indices.
func removeNeedlessFields(row string, indices []int) string {
	elements := strings.Split(row, ",")
	needed := make([]string, 0, len(indices))

	for _, idx := range indices {
		if idx >= 0 && idx < len(elements) {
			needed = append(needed, elements[idx])
		}
	}

	return strings.Join(needed, ",")
}
