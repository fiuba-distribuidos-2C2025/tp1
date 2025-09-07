package common

import (
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type MiddlewareConfig struct {
	Port string
	Ip   string
}

type Middleware struct {
	config MiddlewareConfig
	conns  []net.Conn
}

func NewMiddleware(config MiddlewareConfig) *Middleware {
	middleware := &Middleware{
		config: config,
	}
	return middleware
}

func (m *Middleware) Start() error {
	listener, err := net.Listen("tcp", ":"+m.config.Port)
	if err != nil {
		log.Errorf("Error starting middleware: %s", err)
	}
	defer listener.Close()

	connCount := 0
	log.Infof("Middleware listening on port %s", m.config.Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("Error accepting connection: %s", err)
			continue
		}

		log.Info("Accepted connection from ", conn.RemoteAddr().String())
		m.conns = append(m.conns, conn)

		// THIS IS HERE ONLY TO SHUT OFF WARNING ABOUT
		// CODE NOT REACHABLE
		connCount++
		if connCount > 3 {
			break
		}
	}

	return nil
}
