package common

import (
	"net"
	"strings"
	"time"

	utils "github.com/fiuba-distribuidos-2C2025/tp1/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type MiddlewareConfig struct {
	Port string
	Ip   string
}

type Middleware struct {
	config      MiddlewareConfig
	listener    net.Listener
	workerAddrs []string
	shutdown    chan struct{}
}

func NewMiddleware(config MiddlewareConfig) *Middleware {
	middleware := &Middleware{
		config:   config,
		listener: nil,
		shutdown: make(chan struct{}),
	}
	return middleware
}

func (m *Middleware) HandleNewConnections() {
	for {
		select {
		case <-m.shutdown:
			return
		default:
			// continue execution
		}

		conn, err := m.listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %s", err)
			continue
		}

		log.Info("Accepted connection from ", conn.RemoteAddr().String())
		msg, err := utils.ReceiveMessage(conn)
		if err != nil {
			log.Errorf("Error accepting connection: %s", err)
			continue
		}
		log.Infof("Received message: %s", msg)
		if conn != nil {
			conn.Close()
		}

		// Sleep for a while to simulate work
		time.Sleep(2 * time.Second)
		workerAddr := strings.Split(msg, ",")[1]
		m.workerAddrs = append(m.workerAddrs, workerAddr)
	}
}

func (m *Middleware) DistributeWork() {

	for {
		select {
		case <-m.shutdown:
			return
		default:
			// continue execution
		}

		for i, workerAddr := range m.workerAddrs {
			select {
			case <-m.shutdown:
				return
			default:
				// continue execution
			}

			conn, err := net.Dial("tcp", workerAddr)
			if err != nil {
				log.Error("Lost connection from worker, removing it from the list")
				// Remove worker from list
				m.workerAddrs = append(m.workerAddrs[:i], m.workerAddrs[i+1:]...)
				continue
			}
			log.Info("Sending message to worker", workerAddr)
			conn.Close()
		}

		time.Sleep(2 * time.Second)
	}
}

func (m *Middleware) Start() error {
	listener, err := net.Listen("tcp", ":"+m.config.Port)
	if err != nil {
		log.Errorf("Error starting middleware: %s", err)
	}
	m.listener = listener

	log.Infof("Middleware listening on port %s", m.config.Port)

	// Handle new connections in a separate goroutine
	go func() {
		// TODO: Thread safe?
		m.HandleNewConnections()
	}()

	go func() {
		// TODO: Thread safe?
		m.DistributeWork()
	}()

	// Wait for shutdown signal
	<-m.shutdown
	return nil
}

func (m *Middleware) Stop() {
	log.Info("Shutting down middleware...")

	// Send stop signal to running loop
	close(m.shutdown)

	if m.listener != nil {
		m.listener.Close()
	}

	log.Info("Middleware shut down complete.")
}
