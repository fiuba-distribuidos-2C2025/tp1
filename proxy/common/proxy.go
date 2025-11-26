package common

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/op/go-logging"
)

var proxyLog = logging.MustGetLogger("log")

type ProxyConfig struct {
	Port                string        // Port to listen on for client connections
	IP                  string        // IP of the proxy
	RequestHandlers     []string      // List of request handler addresses (IP:Port)
	HealthCheckInterval time.Duration // How often to check handler health
	BufferSize          int           // Buffer size for messages with the client
}

type Proxy struct {
	Config          ProxyConfig
	listener        net.Listener
	shutdown        chan struct{}
	currentHandler  uint32
	healthyHandlers []string
	handlersMutex   sync.RWMutex
}

// NewProxy creates a new Proxy instance
func NewProxy(config ProxyConfig) *Proxy {
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 5 * time.Second
	}

	return &Proxy{
		Config:          config,
		shutdown:        make(chan struct{}),
		healthyHandlers: make([]string, 0, len(config.RequestHandlers)),
	}
}

func (p *Proxy) Start() error {
	proxyLog.Infof("Starting proxy with config %+v", p.Config)

	p.updateHealthyHandlers()

	if len(p.healthyHandlers) == 0 {
		return fmt.Errorf("no healthy request handlers available")
	}

	addr := net.JoinHostPort(p.Config.IP, p.Config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start proxy listener: %w", err)
	}
	p.listener = listener
	proxyLog.Infof("Proxy listening on %s", addr)

	// Start health check routine
	go p.healthCheckLoop()
	// Start accepting connections
	go p.acceptConnections()

	<-p.shutdown
	return nil
}

func (p *Proxy) Stop() {
	proxyLog.Info("Shutting down proxy...")
	close(p.shutdown)

	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			proxyLog.Errorf("Error closing listener: %v", err)
		}
	}

	proxyLog.Info("Proxy shutdown complete")
}

func (p *Proxy) acceptConnections() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.shutdown:
				return
			default:
				proxyLog.Errorf("Error accepting connection: %v", err)
				continue
			}
		}

		proxyLog.Infof("New client connection from %s", clientConn.RemoteAddr())

		go p.handleConnection(clientConn)
	}
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Get next healthy handler using round-robin
	handlerAddr := p.getNextHandler()
	if handlerAddr == "" {
		proxyLog.Error("No healthy request handlers available")
		return
	}

	proxyLog.Infof("Forwarding connection from %s to handler %s",
		clientConn.RemoteAddr(), handlerAddr)

	handlerConn, err := p.dialWithRetry(handlerAddr)
	if err != nil {
		proxyLog.Error("Failed to connect to any handler after retries")
		return
	}

	defer handlerConn.Close()
	proxyLog.Infof("Established connection to handler %s", handlerAddr)

	p.bidirectionalCopy(clientConn, handlerConn)
	proxyLog.Infof("Connection from %s completed", clientConn.RemoteAddr())
}

func (p *Proxy) bidirectionalCopy(clientConn, handlerConn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Copy from client to handler
	go func() {
		defer wg.Done()
		io.Copy(handlerConn, clientConn)
		if tcpConn, ok := handlerConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	// Copy from handler to client
	go func() {
		defer wg.Done()
		io.Copy(clientConn, handlerConn)
		if tcpConn, ok := clientConn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
	}()

	wg.Wait()
}

func (p *Proxy) dialWithRetry(initialAddr string) (net.Conn, error) {
	handlerAddr := initialAddr

	for {
		handlerConn, err := net.DialTimeout("tcp", handlerAddr, 5*time.Second)
		if err == nil {
			return handlerConn, nil
		}

		proxyLog.Errorf("Failed to connect to handler %s: %v", handlerAddr, err)
		p.removeUnhealthyHandler(handlerAddr)

		handlerAddr = p.getNextHandler()
		if handlerAddr == "" {
			return nil, fmt.Errorf("no healthy request handlers available after retries")
		}
	}
}

func (p *Proxy) getNextHandler() string {
	p.handlersMutex.RLock()
	defer p.handlersMutex.RUnlock()

	numHandlers := len(p.healthyHandlers)
	if numHandlers == 0 {
		return ""
	}

	counter := atomic.AddUint32(&p.currentHandler, 1)
	idx := (counter - 1) % uint32(numHandlers)
	return p.healthyHandlers[idx]
}

func (p *Proxy) healthCheckLoop() {
	ticker := time.NewTicker(p.Config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdown:
			return
		case <-ticker.C:
			p.updateHealthyHandlers()
		}
	}
}

func (p *Proxy) updateHealthyHandlers() {
	proxyLog.Debug("Performing health check on request handlers")

	var healthy []string

	for _, handler := range p.Config.RequestHandlers {
		if p.isHealthy(handler) {
			healthy = append(healthy, handler)
		} else {
			proxyLog.Warningf("Handler %s is unhealthy", handler)
		}
	}

	p.handlersMutex.Lock()
	p.healthyHandlers = healthy
	p.handlersMutex.Unlock()

	proxyLog.Infof("Health check complete: %d/%d handlers healthy",
		len(healthy), len(p.Config.RequestHandlers))
}

func (p *Proxy) isHealthy(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (p *Proxy) removeUnhealthyHandler(addr string) {
	p.handlersMutex.Lock()
	defer p.handlersMutex.Unlock()

	for i, handler := range p.healthyHandlers {
		if handler == addr {
			p.healthyHandlers = append(p.healthyHandlers[:i], p.healthyHandlers[i+1:]...)
			proxyLog.Warningf("Removed unhealthy handler: %s", addr)
			break
		}
	}
}
