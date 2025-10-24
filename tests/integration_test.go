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

// Creates all necessary queues for the test
func setupQueuesFirstQuery(factory *middleware.MockQueueFactory, numYearWorkers, numHourWorkers int) (
	firstInputQueues []*middleware.MockMessageMiddleware,
	finalOutput *middleware.MockMessageMiddleware,
) {
	firstInputQueues = make([]*middleware.MockMessageMiddleware, numYearWorkers)
	
	// Create input queues for year filter workers
	for i := 1; i <= numYearWorkers; i++ {
		firstInputQueues[i-1] = factory.CreateQueue(fmt.Sprintf("transactions_1_%d", i)).(*middleware.MockMessageMiddleware)
		factory.CreateQueue(fmt.Sprintf("transactions_2024_2025_q1_%d", i))
	}

	// Create queues for hour filter workers
	for i := 1; i <= numHourWorkers; i++ {
		factory.CreateQueue(fmt.Sprintf("transactions_2024_2025_q1_%d", i))
		factory.CreateQueue(fmt.Sprintf("transactions_filtered_by_hour_q1_%d", i))
	}

	// Create queues for amount filter workers
	for i := 1; i <= numHourWorkers; i++ {
		factory.CreateQueue(fmt.Sprintf("transactions_filtered_by_hour_q1_%d", i))
	}
	
	factory.CreateQueue("results_1_1")
	finalOutput = factory.CreateQueue("final_results_1_1").(*middleware.MockMessageMiddleware)
	
	return firstInputQueues, finalOutput
}

// Sends test data to the input queues
func sendTestData(queues []*middleware.MockMessageMiddleware, testData []struct {
	input       string
	shouldPass  bool
	description string
}) {
	for _, td := range testData {
		for _, queue := range queues {
			queue.Send([]byte(td.input))
		}
	}
}

// Creates the worker configuration
func createWorkerConfig(inputQueue, outputQueue string, inputSenders, outputReceivers []string, workerJob string, id int) worker.WorkerConfig {
	return worker.WorkerConfig{
		MiddlewareUrl:   "test",
		InputQueue:      []string{inputQueue},
		OutputQueue:     []string{outputQueue},
		InputSenders:    inputSenders,
		OutputReceivers: outputReceivers,
		WorkerJob:       workerJob,
		ID:              id,
		IsTest:          true,
	}
}

// Creates and starts multiple workers with a list of configurations
func startWorkers(t *testing.T, factory *middleware.MockQueueFactory, configs []worker.WorkerConfig) []*worker.Worker {
	workers := make([]*worker.Worker, len(configs))
	
	for i, config := range configs {
		w, err := worker.NewWorker(config, factory)
		if err != nil {
			t.Fatalf("Failed to create worker %d: %v", i+1, err)
		}
		go w.Start()
		workers[i] = w
	}
	
	return workers
}

// Stops all test workers
func stopWorkers(workers []*worker.Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

// Creates and starts a response builder
func createResponseBuilder(t *testing.T, factory *middleware.MockQueueFactory, resultsOneCount int) *response_builder.ResponseBuilder {
	config := response_builder.ResponseBuilderConfig{
		MiddlewareUrl:           "test",
		WorkerResultsOneCount:   resultsOneCount,
		WorkerResultsTwoCount:   1,
		WorkerResultsThreeCount: 1,
		WorkerResultsFourCount:  1,
		IsTest:                  true,
	}

	rb := response_builder.NewResponseBuilder(config, factory)
	go rb.Start()
	
	return rb
}

// Sets up and runs the entire worker pipeline
func runWorkersFirstQuery(t *testing.T, factory *middleware.MockQueueFactory, numYearWorkers, numHourWorkers, numAmountWorkers int, inputSendersYear, outputReceiversYear []string, inputSendersHour, outputReceiversHour []string, inputSendersAmount, outputReceiversAmount []string) []*worker.Worker {
	var allWorkers []*worker.Worker

	// Create year filter workers
	yearConfigs := make([]worker.WorkerConfig, numYearWorkers)
	for i := 1; i <= numYearWorkers; i++ {
		yearConfigs[i-1] = createWorkerConfig("transactions_1", "transactions_2024_2025_q1", inputSendersYear, outputReceiversYear, "YEAR_FILTER", i)
	}
	yearWorkers := startWorkers(t, factory, yearConfigs)
	allWorkers = append(allWorkers, yearWorkers...)
	time.Sleep(5 * time.Second)

	// Create hour filter workers
	hourConfigs := make([]worker.WorkerConfig, numHourWorkers)
	for i := 1; i <= numHourWorkers; i++ {
		hourConfigs[i-1] = createWorkerConfig("transactions_2024_2025_q1", "transactions_filtered_by_hour_q1", inputSendersHour, outputReceiversHour, "HOUR_FILTER", i)
	}
	hourWorkers := startWorkers(t, factory, hourConfigs)
	allWorkers = append(allWorkers, hourWorkers...)
	time.Sleep(5 * time.Second)

	// Create amount filter workers
	amountConfigs := make([]worker.WorkerConfig, numAmountWorkers)
	for i := 1; i <= numAmountWorkers; i++ {
		amountConfigs[i-1] = createWorkerConfig("transactions_filtered_by_hour_q1", "results_1", inputSendersAmount, outputReceiversAmount, "AMOUNT_FILTER", i)
	}
	amountWorkers := startWorkers(t, factory, amountConfigs)
	allWorkers = append(allWorkers, amountWorkers...)
	time.Sleep(5 * time.Second)

	return allWorkers
}

func TestFirstQuery(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	firstInputQueues, finalOutput := setupQueuesFirstQuery(factory, 1, 1)

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

	sendTestData(firstInputQueues, testData)

	workers := runWorkersFirstQuery(t, factory, 1, 1, 1, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"})
	defer stopWorkers(workers)

	createResponseBuilder(t, factory, 1)
	time.Sleep(5 * time.Second)
	
	outputMessages := finalOutput.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 1, len(outputMessages), "Output queue should have exactly 1 message")
}

func TestEOF(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	firstInputQueues, _ := setupQueuesFirstQuery(factory, 1, 1)
	thirdOutputQueue := factory.CreateQueue("results_1_1").(*middleware.MockMessageMiddleware)

	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\nEOF", false, "EOF marker"},
	}

	sendTestData(firstInputQueues, testData)

	workers := runWorkersFirstQuery(t, factory, 1, 1, 1, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"}, []string{"1"})
	defer stopWorkers(workers)

	createResponseBuilder(t, factory, 2)
	time.Sleep(5 * time.Second)
	
	outputMessages := thirdOutputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 1, len(outputMessages), "Output queue should have exactly 1 message")
}

func TestEOFWithManyInputWorkers(t *testing.T) {
	factory := middleware.NewMockQueueFactory()
	firstInputQueues, _ := setupQueuesFirstQuery(factory, 2, 2)
	thirdOutputQueue := factory.CreateQueue("results_1_1").(*middleware.MockMessageMiddleware)

	testData := []struct {
		input       string
		shouldPass  bool
		description string
	}{
		{"1\nEOF", false, "EOF marker"},
	}

	sendTestData(firstInputQueues, testData)

	workers := runWorkersFirstQuery(t, factory, 2, 2, 2, []string{"1"}, []string{"2"}, []string{"2"}, []string{"2"}, []string{"2"}, []string{"1"})
	defer stopWorkers(workers)

	createResponseBuilder(t, factory, 1)
	time.Sleep(5 * time.Second)
	
	outputMessages := thirdOutputQueue.GetMessages()
	fmt.Printf("Output Messages: %s\n", outputMessages)
	assert.Equal(t, 2, len(outputMessages), "Output queue should have exactly 2 messages")
}