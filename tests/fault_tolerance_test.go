package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	worker "github.com/fiuba-distribuidos-2C2025/tp1/worker/common"
	"github.com/stretchr/testify/assert"
)

func TestRestartAggregatorBySemester(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	inputQueue := make([]*middleware.MockMessageMiddleware, 1)
	inputQueue[0] = factory.CreateQueue("semester_aggregator_queue_1").(*middleware.MockMessageMiddleware)
	outputQueue := factory.CreateQueue("semester_grouped_transactions_1").(*middleware.MockMessageMiddleware)

	// We use msgID = 999
	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\n999\n1-2024-H1,2024,H1,1,100", true, "Valid transaction"},
	}

	SendTestData(inputQueue, testData)

	config := CreateWorkerConfig("semester_aggregator_queue", "semester_grouped_transactions", []string{"1"}, []string{"1"}, "AGGREGATOR_SEMESTER", 1)

	w, err := worker.NewWorker(config, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go w.Start()

	time.Sleep(5 * time.Second)

	outputMessages := outputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 0, len(outputMessages), "Output queue should be empty. EOF not reached yet.")

	// Restart worker
	w.Stop()

	w, err = worker.NewWorker(config, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	// Send EOF message
	testData = []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\n1\nEOF", true, "EOF"},
	}

	SendTestData(inputQueue, testData)
	go w.Start()

	time.Sleep(5 * time.Second)

	outputMessages = outputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 2, len(outputMessages), "Output queue should have 2 messages after EOF.")
	expectedOutput := "1\n1-2024-H1,2024,H1,1,100.00\n"
	assert.Equal(t, expectedOutput, string(outputMessages[0]), "Output message content mismatch.")
	assert.Equal(t, "1\nEOF", string(outputMessages[1]), "Output message content mismatch.")
}

func TestRepeatedMessagesAggregatorBySemester(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	inputQueue := make([]*middleware.MockMessageMiddleware, 1)
	inputQueue[0] = factory.CreateQueue("semester_aggregator_queue_1").(*middleware.MockMessageMiddleware)
	outputQueue := factory.CreateQueue("semester_grouped_transactions_1").(*middleware.MockMessageMiddleware)

	// We use msgID = 999
	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\n999\n1-2024-H1,2024,H1,1,100", true, "Valid transaction"},
		{"1\n999\n1-2024-H1,2024,H1,1,100", true, "Valid transaction"},
	}

	SendTestData(inputQueue, testData)

	config := CreateWorkerConfig("semester_aggregator_queue", "semester_grouped_transactions", []string{"1"}, []string{"1"}, "AGGREGATOR_SEMESTER", 1)

	w, err := worker.NewWorker(config, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go w.Start()

	time.Sleep(5 * time.Second)

	outputMessages := outputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 0, len(outputMessages), "Output queue should be empty. EOF not reached yet.")

	// Restart worker
	w.Stop()

	w, err = worker.NewWorker(config, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	// Send EOF message
	testData = []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\n999\n1-2024-H1,2024,H1,1,100", true, "Valid transaction"},
		{"1\n1\nEOF", true, "EOF"},
	}

	SendTestData(inputQueue, testData)
	go w.Start()

	time.Sleep(5 * time.Second)

	outputMessages = outputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 2, len(outputMessages), "Output queue should have 2 messages after EOF.")
	expectedOutput := "1\n1-2024-H1,2024,H1,1,100.00\n"
	assert.Equal(t, expectedOutput, string(outputMessages[0]), "Output message content mismatch.")
	assert.Equal(t, "1\nEOF", string(outputMessages[1]), "Output message content mismatch.")
}
