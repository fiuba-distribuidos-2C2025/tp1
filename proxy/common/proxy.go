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

// ProxyConfig holds configuration for the proxy
type ProxyConfig struct {
	Port             string   // Port to listen on for client connections
	IP               string   // IP to bind to
	RequestHandlers  []string // List of request handler addresses (IP:Port)
	HealthCheckInterval time.Duration // How often to check handler health
	BufferSize       int      // Buffer size for client connections
}

// Proxy acts as a load balancer for request handlers
type Proxy struct {
	Config           ProxyConfig
	listener         net.Listener
	shutdown         chan struct{}
	currentHandler   uint32 // Atomic counter for round-robin
	healthyHandlers  []string
	handlersMutex    sync.RWMutex
	wg               sync.WaitGroup
}

// NewProxy creates a new Proxy instance
func NewProxy(config ProxyConfig) *Proxy {
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 10 * time.Second
	}

	return &Proxy{
		Config:          config,
		shutdown:        make(chan struct{}),
		healthyHandlers: make([]string, 0, len(config.RequestHandlers)),
	}
}

// Start begins the proxy operations
func (p *Proxy) Start() error {
	proxyLog.Infof("Starting proxy with config %+v", p.Config)

	// Initial health check
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
	p.wg.Add(1)
	go p.healthCheckLoop()

	// Start accepting connections
	go p.acceptConnections()

	<-p.shutdown
	return nil
}

// Stop gracefully shuts down the proxy
func (p *Proxy) Stop() {
	proxyLog.Info("Shutting down proxy...")
	close(p.shutdown)

	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			proxyLog.Errorf("Error closing listener: %v", err)
		}
	}

	p.wg.Wait()
	proxyLog.Info("Proxy shutdown complete")
}

// acceptConnections continuously accepts new client connections
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

		p.wg.Add(1)
		go p.handleConnection(clientConn)
	}
}

// handleConnection forwards a client connection to a request handler
func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	// Get next healthy handler using round-robin
	handlerAddr := p.getNextHandler()
	if handlerAddr == "" {
		proxyLog.Error("No healthy request handlers available")
		return
	}

	proxyLog.Infof("Forwarding connection from %s to handler %s",
		clientConn.RemoteAddr(), handlerAddr)

	// Connect to the request handler
	handlerConn, err := net.DialTimeout("tcp", handlerAddr, 5*time.Second)
	if err != nil {
		proxyLog.Errorf("Failed to connect to handler %s: %v", handlerAddr, err)
		p.removeUnhealthyHandler(handlerAddr)

		handlerAddr = p.getNextHandler()
		if handlerAddr == "" {
			proxyLog.Error("No healthy request handlers available after retry")
			return
		}

		handlerConn, err = net.DialTimeout("tcp", handlerAddr, 5*time.Second)
		if err != nil {
			proxyLog.Errorf("Failed to connect to backup handler %s: %v", handlerAddr, err)
			return
		}
	}
	defer handlerConn.Close()

	proxyLog.Infof("Established connection to handler %s", handlerAddr)

	// Bidirectional copy between client and handler
	done := make(chan struct{})

	// Copy from client to handler
	go func() {
		io.Copy(handlerConn, clientConn)
		// Signal that client->handler is done, but don't close yet
		done <- struct{}{}
	}()

	// Copy from handler to client
	go func() {
		io.Copy(clientConn, handlerConn)
		// Signal that handler->client is done
		done <- struct{}{}
	}()

	// Wait for BOTH directions to complete naturally
	<-done
	<-done

	proxyLog.Infof("Connection from %s completed", clientConn.RemoteAddr())
}

func (p *Proxy) getNextHandler() string {
    p.handlersMutex.RLock()
    numHandlers := len(p.healthyHandlers)
    if numHandlers == 0 {
        p.handlersMutex.RUnlock()
        return ""
    }

    // Atomically get and increment, then calculate index
    counter := atomic.AddUint32(&p.currentHandler, 1)
    idx := (counter - 1) % uint32(numHandlers)
    handler := p.healthyHandlers[idx]
    p.handlersMutex.RUnlock()

    return handler
}

// healthCheckLoop periodically checks handler health
func (p *Proxy) healthCheckLoop() {
	defer p.wg.Done()

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

// updateHealthyHandlers checks all handlers and updates the healthy list
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

// isHealthy checks if a handler is responsive
func (p *Proxy) isHealthy(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// removeUnhealthyHandler temporarily removes a handler from the healthy list
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

// GetHealthyHandlerCount returns the current number of healthy handlers
func (p *Proxy) GetHealthyHandlerCount() int {
	p.handlersMutex.RLock()
	defer p.handlersMutex.RUnlock()
	return len(p.healthyHandlers)
}
