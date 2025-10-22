package integration_test

import (
	"testing"
	"time"
	"fmt"
	"github.com/stretchr/testify/assert"
	worker "github.com/fiuba-distribuidos-2C2025/tp1/worker/common"
	middleware "github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	response_builder "github.com/fiuba-distribuidos-2C2025/tp1/response_builder/common"
)



// TestFirstQuery tests the filter using mock middleware
func TestFirstQuery(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	firstInputQueue := factory.CreateQueue("transactions_1_1").(*middleware.MockMessageMiddleware)
	_ = factory.CreateQueue("transactions_2024_2025_q1_1").(*middleware.MockMessageMiddleware)

	_ = factory.CreateQueue("transactions_2024_2025_q1_1").(*middleware.MockMessageMiddleware)
	_ = factory.CreateQueue("transactions_filtered_by_hour_q1_1").(*middleware.MockMessageMiddleware)

	_ = factory.CreateQueue("transactions_filtered_by_hour_q1_1").(*middleware.MockMessageMiddleware)
	_ = factory.CreateQueue("results_1_1").(*middleware.MockMessageMiddleware)

	_ = factory.CreateQueue("results_1_1").(*middleware.MockMessageMiddleware)
	finalOutput := factory.CreateQueue("final_results_1_1").(*middleware.MockMessageMiddleware)

	// Send test data
	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\nac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,90.0,2024-01-01 10:06:50", true, "above threshold"},
		{"1\nd7856e66-c613-45c4-b9ca-7a3cec6c6db3,8,2,,,30.5,0.0,80.0,2024-01-01 10:06:52", true, "below threshold"},
		{"1\n78015742-1f8b-4f9c-bde2-0e68b822890c,5,4,,,54.0,0.0,90.0,2024-01-01 10:06:53", true, "above threshold"},
		{"1\nda1b334f-a51b-421d-88ce-ca547bc1cdbe,1,1,,,48.0,0.0,90.0,2024-01-01 10:06:54", true, "below threshold"},
		{"1\nEOF", false, "EOF marker"},
	}

	for _, td := range testData {
		firstInputQueue.Send([]byte(td.input))
	}

	yearFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_1"},
		OutputQueue:  []string{"transactions_2024_2025_q1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "YEAR_FILTER",
		ID:           1,
		IsTest:       true,
	}

	hourFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_2024_2025_q1"},
		OutputQueue:  []string{"transactions_filtered_by_hour_q1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "HOUR_FILTER",
		ID:           1,
		IsTest:       true,
	}

	amountFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_filtered_by_hour_q1"},
		OutputQueue:  []string{"results_1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "AMOUNT_FILTER",
		ID:           1,
		IsTest:       true,
	}

	responseBuilderConfig := response_builder.ResponseBuilderConfig{
		MiddlewareUrl:           "test",
		WorkerResultsOneCount:   1,
		WorkerResultsTwoCount:   1,
		WorkerResultsThreeCount: 1,
		WorkerResultsFourCount:  1,
		IsTest:				  true,
	}

	yearWorker, err := worker.NewWorker(yearFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go yearWorker.Start()
	defer yearWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	hourWorker, err := worker.NewWorker(hourFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go hourWorker.Start()
	defer hourWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	amountWorker, err := worker.NewWorker(amountFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go amountWorker.Start()
	defer amountWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	responseBuilder := response_builder.NewResponseBuilder(responseBuilderConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create response builder: %v", err)
	}
	go responseBuilder.Start()

	time.Sleep(5 * time.Second) // Wait for processing
	
	// Verify messages in output queue
	outputMessages := finalOutput.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 1, len(outputMessages), "Output queue should have exactly 1 message")
}

// TestEOF tests the filter using mock middleware
func TestEOF(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	firstInputQueue := factory.CreateQueue("transactions_1_1").(*middleware.MockMessageMiddleware)
	_ = factory.CreateQueue("transactions_2024_2025_q1_1").(*middleware.MockMessageMiddleware)

	_ = factory.CreateQueue("transactions_2024_2025_q1_1").(*middleware.MockMessageMiddleware)
	_ = factory.CreateQueue("transactions_filtered_by_hour_q1_1").(*middleware.MockMessageMiddleware)

	_ = factory.CreateQueue("transactions_filtered_by_hour_q1_1").(*middleware.MockMessageMiddleware)
	thirdOutputQueue := factory.CreateQueue("results_1_1").(*middleware.MockMessageMiddleware)

	// Send test data
	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\nEOF", false, "EOF marker"},
	}

	for _, td := range testData {
		firstInputQueue.Send([]byte(td.input))
	}

	yearFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_1"},
		OutputQueue:  []string{"transactions_2024_2025_q1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "YEAR_FILTER",
		ID:           1,
		IsTest:       true,
	}

	hourFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_2024_2025_q1"},
		OutputQueue:  []string{"transactions_filtered_by_hour_q1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "HOUR_FILTER",
		ID:           1,
		IsTest:       true,
	}

	amountFilterTestWorkerConfig := worker.WorkerConfig{
		MiddlewareUrl: "test",
		InputQueue:   []string{"transactions_filtered_by_hour_q1"},
		OutputQueue:  []string{"results_1"},
		InputSenders:    []string{"1"},
		OutputReceivers: []string{"1"},
		WorkerJob:    "AMOUNT_FILTER",
		ID:           1,
		IsTest:       true,
	}

	responseBuilderConfig := response_builder.ResponseBuilderConfig{
		MiddlewareUrl:           "test",
		WorkerResultsOneCount:   1,
		WorkerResultsTwoCount:   1,
		WorkerResultsThreeCount: 1,
		WorkerResultsFourCount:  1,
		IsTest:				  true,
	}

	yearWorker, err := worker.NewWorker(yearFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go yearWorker.Start()
	defer yearWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	hourWorker, err := worker.NewWorker(hourFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go hourWorker.Start()
	defer hourWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	amountWorker, err := worker.NewWorker(amountFilterTestWorkerConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	go amountWorker.Start()
	defer amountWorker.Stop()

	time.Sleep(5 * time.Second) // Wait for processing

	responseBuilder := response_builder.NewResponseBuilder(responseBuilderConfig, factory)
	if err != nil {
		t.Fatalf("Failed to create response builder: %v", err)
	}
	go responseBuilder.Start()

	time.Sleep(5 * time.Second) // Wait for processing
	
	// Verify messages in output queue
	outputMessages := thirdOutputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 1, len(outputMessages), "Output queue should have exactly 1 message")
}