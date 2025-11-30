package healthcheck

import (
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"net/http"
	"sync"
	"time"
)

var log = logging.MustGetLogger("log")

type HealthStatus struct {
	Status    string    `json:"status"`
	Uptime    string    `json:"uptime"`
	Timestamp time.Time `json:"timestamp"`
}

type HealthChecker struct {
	startTime time.Time
	mu        sync.RWMutex
	isHealthy bool
}

var healthChecker *HealthChecker

func InitHealthChecker() {
	healthChecker = &HealthChecker{
		startTime: time.Now(),
		isHealthy: true,
	}
}

func (h *HealthChecker) SetHealthy(healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.isHealthy = healthy
}

func (h *HealthChecker) IsHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isHealthy
}

func (h *HealthChecker) GetStatus() HealthStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	status := "healthy"
	if !h.isHealthy {
		status = "unhealthy"
	}

	uptime := time.Since(h.startTime).String()

	return HealthStatus{
		Status:    status,
		Uptime:    uptime,
		Timestamp: time.Now(),
	}
}

func StartHealthCheckServer(port int) {
	http.HandleFunc("/health", healthCheckHandler)

	go func() {
		addr := fmt.Sprintf(":%d", port)
		log.Infof("Starting health check server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Health check server error: %v", err)
		}
	}()
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if healthChecker == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "health checker not initialized"})
		return
	}

	status := healthChecker.GetStatus()

	w.Header().Set("Content-Type", "application/json")

	if status.Status == "healthy" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(status)
}
