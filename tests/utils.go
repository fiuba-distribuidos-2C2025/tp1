package tests

import (
	"github.com/fiuba-distribuidos-2C2025/tp1/middleware"
	worker "github.com/fiuba-distribuidos-2C2025/tp1/worker/common"
)

// Sends test data to the input queues
func SendTestData(queues []*middleware.MockMessageMiddleware, testData []struct {
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
func CreateWorkerConfig(inputQueue, outputQueue string, inputSenders, outputReceivers []string, workerJob string, id int) worker.WorkerConfig {
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
