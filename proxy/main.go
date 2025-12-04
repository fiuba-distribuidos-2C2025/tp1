package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fiuba-distribuidos-2C2025/tp1/healthcheck"
	"github.com/fiuba-distribuidos-2C2025/tp1/proxy/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const (
	defaultConfigPath = "/config/config.yaml"
	envPrefix         = "proxy"
	defaultLogLevel   = "info"
)

// initConfig initializes Viper configuration from file and environment variables
func initConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure environment variable handling
	v.AutomaticEnv()
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Bind specific environment variables
	configKeys := []string{
		"proxy.port",
		"proxy.ip",
		"log.level",
		"proxy.buffer_size_mb",
	}

	for _, key := range configKeys {
		if err := v.BindEnv(key); err != nil {
			return nil, fmt.Errorf("failed to bind env var %s: %w", key, err)
		}
	}

	// Set defaults
	v.SetDefault("log.level", defaultLogLevel)
	v.SetDefault("proxy.port", "12345")
	v.SetDefault("proxy.ip", "0.0.0.0")

	// Try to read config file (optional)
	v.SetConfigFile(defaultConfigPath)
	if err := v.ReadInConfig(); err != nil {
		log.Infof("Config file not found, using environment variables and defaults")
	} else {
		log.Infof("Configuration loaded from %s", defaultConfigPath)
	}

	return v, nil
}

// initLogger configures the logging backend with the specified log level
func initLogger(logLevel string) error {
	// Parse log level
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level '%s': %w", logLevel, err)
	}

	// Create log backend
	backend := logging.NewLogBackend(os.Stdout, "", 0)

	// Define log format
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)

	// Apply format and level
	formattedBackend := logging.NewBackendFormatter(backend, format)
	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(logLevelCode, "")

	// Set the backend
	logging.SetBackend(leveledBackend)

	log.Infof("Logger initialized with level: %s", logLevel)
	return nil
}

// buildProxyConfig extracts configuration from Viper and builds the config struct
func buildProxyConfig(v *viper.Viper) common.ProxyConfig {
	requestHandlers := v.GetString("requesthandlers.addresses")
	requestHandlersList := strings.Split(requestHandlers, ",")
	return common.ProxyConfig{
		Port:                v.GetString("proxy.port"),
		IP:                  v.GetString("proxy.ip"),
		BufferSize:          v.GetInt("proxy.buffer_size_mb") * 1024 * 1024,
		HealthCheckInterval: time.Duration(v.GetInt("proxy.health_check_interval")) * time.Second,
		RequestHandlers:     requestHandlersList,
	}
}

// runWithGracefulShutdown starts the proxy and handles shutdown signals
func runWithGracefulShutdown(proxy *common.Proxy) error {
	// Set up signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start proxy in goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Infof("Starting proxy on %s:%s",
			proxy.Config.IP,
			proxy.Config.Port)

		if err := proxy.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-signalChan:
		log.Infof("Received signal: %v, initiating graceful shutdown", sig)
		proxy.Stop()
		return nil
	case err := <-errChan:
		return fmt.Errorf("proxy error: %w", err)
	}
}

func main() {
	// Initialize configuration
	v, err := initConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logLevel := v.GetString("log.level")
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	if err := initLogger(logLevel); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Create and start proxy
	config := buildProxyConfig(v)
	proxy := common.NewProxy(config)

	healthcheck.StartHealthCheckServer(9090)

	// Set up graceful shutdown
	if err := runWithGracefulShutdown(proxy); err != nil {
		log.Criticalf("Proxy error: %v", err)
		os.Exit(1)
	}

	log.Info("Proxy shut down gracefully")
}
