package common

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

type WorkerConfig struct {
	MiddlewareAddress string
	Ip                string
}

type Worker struct {
	config   WorkerConfig
	shutdown chan struct{}
	conn     *amqp.Connection
	channel  *amqp.Channel
}

func NewWorker(config WorkerConfig) (*Worker, error) {
	time.Sleep(10 * time.Second)                                            // wait for rabbitmq to be ready
	conn, err := amqp.Dial("amqp://guest:guest@host.docker.internal:5672/") // pass from config
	if err != nil {
		log.Errorf("Failed to connect to RabbitMQ: %v", err)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("Failed to open a rabbitmq channel: %v", err)
		return nil, err
	}
	worker := &Worker{
		config:   config,
		shutdown: make(chan struct{}),
		conn:     conn,
		channel:  ch,
	}
	return worker, nil
}

func (w *Worker) Start() error {

	in_queue, err := w.channel.QueueDeclare(
		"input", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	if err != nil {
		log.Errorf("Failed to declare in_queue: %v", err)
		return err
	}

	out_queue, err := w.channel.QueueDeclare(
		"output", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		log.Errorf("Failed to declare a out_queue: %v", err)
		return err
	}

	var total int64

	msgs, err := w.channel.Consume(
		in_queue.Name, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)

	if err != nil {
		log.Errorf("Failed to register a consumer: %v", err)
	}

	for {
		select {
		case <-w.shutdown:
			log.Info("Shutdown signal received, stopping worker...")
			return nil
		case msg, ok := <-msgs:
			if !ok {
				log.Infof("Deliveries channel closed; shutting down")
				return nil
			}
			log.Infof("Received a message: %s", msg.Body)

			body := strings.TrimSpace(string(msg.Body))
			if strings.EqualFold(body, "fin") {
				handleFinMsg(w.channel, out_queue, &total)
				if err != nil {
					log.Errorf("Failed to handle fin message: %v", err)
					return err
				}
				continue
			}

			n, parseErr := strconv.ParseInt(body, 10, 64)
			if parseErr != nil {
				log.Errorf("[worker] invalid integer %q: %v (dropping message)", body, parseErr)
				continue
			}
			total += n
		}
	}
}

func handleFinMsg(ch *amqp.Channel, out_q amqp.Queue, total *int64) error {
	// Publish the accumulated total, then reset
	payload := strconv.FormatInt(*total, 10)

	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := ch.PublishWithContext(
		pubCtx,
		"",
		out_q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain", DeliveryMode: amqp.Persistent,
			Body: []byte(payload),
		},
	)

	if err != nil {
		log.Infof("[worker] Publish error (sum=%d): %v", total, err)
		return err
	}

	log.Infof("[worker] FIN received; published total=%d; resetting", *total)
	*total = 0 // reset
	return nil
}

func (w *Worker) Stop() {
	log.Info("Shutting down worker...")

	// Send stop signal to running loop
	close(w.shutdown)
	w.channel.Close()
	w.conn.Close()

	log.Info("Worker shut down complete.")
}
