package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fiuba-distribuidos-2C2025/tp1/request_handler/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const (
	defaultConfigPath = "/config/config.yaml"
	envPrefix         = "request"
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
		"request_handler.port",
		"request_handler.ip",
		"request_handler.url",
		"log.level",
		"request_handler.buffer_size_mb",
	}

	for _, key := range configKeys {
		if err := v.BindEnv(key); err != nil {
			return nil, fmt.Errorf("failed to bind env var %s: %w", key, err)
		}
	}

	// Set defaults
	v.SetDefault("log.level", defaultLogLevel)
	v.SetDefault("request_handler.port", "8080")
	v.SetDefault("request_handler.ip", "0.0.0.0")
	v.SetDefault("rabbit.url", "amqp://guest:guest@localhost:5672/")

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

// buildRequestHandlerConfig extracts configuration from Viper and builds the config struct
func buildRequestHandlerConfig(v *viper.Viper) common.RequestHandlerConfig {
	return common.RequestHandlerConfig{
		Port:                           v.GetString("request_handler.port"),
		IP:                             v.GetString("request_handler.ip"),
		MiddlewareURL:                  v.GetString("rabbit.url"),
		TransactionsReceiversCount:     v.GetInt("middleware.receivers.transactionscount"),
		TransactionItemsReceiversCount: v.GetInt("middleware.receivers.transactionitemscount"),
		StoresQ3ReceiversCount:         v.GetInt("middleware.receivers.storesq3count"),
		StoresQ4ReceiversCount:         v.GetInt("middleware.receivers.storesq4count"),
		MenuItemsReceiversCount:        v.GetInt("middleware.receivers.menuitemscount"),
		UsersReceiversCount:            v.GetInt("middleware.receivers.userscount"),
		BufferSize:                     v.GetInt("request_handler.buffer_size_mb") * 1024 * 1024,
	}
}

// runWithGracefulShutdown starts the request handler and handles shutdown signals
func runWithGracefulShutdown(requestHandler *common.RequestHandler) error {
	// Set up signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start request handler in goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Infof("Starting request handler on %s:%s",
			requestHandler.Config.IP,
			requestHandler.Config.Port)

		if err := requestHandler.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-signalChan:
		log.Infof("Received signal: %v, initiating graceful shutdown", sig)
		requestHandler.Stop()
		return nil
	case err := <-errChan:
		return fmt.Errorf("request handler error: %w", err)
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

	// Create and start request handler
	config := buildRequestHandlerConfig(v)
	requestHandler := common.NewRequestHandler(config)

	// Set up graceful shutdown
	if err := runWithGracefulShutdown(requestHandler); err != nil {
		log.Criticalf("Request handler error: %v", err)
		os.Exit(1)
	}

	log.Info("Request handler shut down gracefully")
}
