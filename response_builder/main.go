package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fiuba-distribuidos-2C2025/tp1/response_builder/common"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
func InitConfig() (*viper.Viper, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvPrefix("response")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("rabbitmq", "url")
	v.BindEnv("log", "level")

	v.SetConfigFile("/config/config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead\n")
	}

	// Set defaults
	v.SetDefault("rabbitmq.url", "amqp://guest:guest@localhost:5672/")
	v.SetDefault("rabbitmq.exchange", "results_exchange")
	v.SetDefault("rabbitmq.final_queue", "final_results")

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string.
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")
	logging.SetBackend(backendLeveled)
	return nil
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Set up signal handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	if err != nil {
		log.Criticalf("Failed to initialize RabbitMQ handler: %s", err)
		os.Exit(1)
	}

	// Start original request handler
	requestBuilderConfig := common.ResponseBuilderConfig{
		MiddlewareUrl: v.GetString("rabbit.url"),
		WorkerCount:   v.GetInt("middleware.receivers.count"),
	}
	requestBuilder := common.NewResponseBuilder(requestBuilderConfig)

	go func() {
		requestBuilder.Start()
	}()

	// Wait for shutdown signal
	<-stop

	log.Info("Shutdown signal received, cleaning up...")

	log.Info("Shutdown complete")
}

// package main

// import (
// 	"fmt"
// 	"os"
// 	"os/signal"
// 	"strings"
// 	"syscall"

// 	"github.com/fiuba-distribuidos-2C2025/tp1/response_builder/common"
// 	"github.com/op/go-logging"
// 	"github.com/spf13/viper"
// )

// var log = logging.MustGetLogger("log")

// const (
// 	defaultRabbitURL = "amqp://guest:guest@localhost:5672/"
// 	configFilePath   = "/config/config.yaml"
// )

// // InitConfig parses configuration parameters using viper.
// func InitConfig() *viper.Viper {
// 	v := viper.New()
// 	v.AutomaticEnv()
// 	v.SetEnvPrefix("cli")
// 	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

// 	// Supported environment variables
// 	_ = v.BindEnv("rabbitmq.url")
// 	_ = v.BindEnv("rabbitmq.exchange")
// 	_ = v.BindEnv("rabbitmq.final_queue")
// 	_ = v.BindEnv("log.level")

// 	v.SetConfigFile("/config/config.yaml")
// 	if err := v.ReadInConfig(); err != nil {
// 		fmt.Println("Config file not found, falling back to environment variables")
// 	}

// 	// Defaults
// 	v.SetDefault("rabbitmq.url", "amqp://guest:guest@localhost:5672/")

// 	return v
// }

// // InitLogger sets up the global logger with the provided log level.
// func InitLogger(logLevel string) error {
// 	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
// 	format := logging.MustStringFormatter(
// 		`%{time:2006-01-02 15:04:05} %{level:.5s} %{message}`,
// 	)
// 	backendFormatter := logging.NewBackendFormatter(baseBackend, format)
// 	backendLeveled := logging.AddModuleLevel(backendFormatter)

// 	level, err := logging.LogLevel(logLevel)
// 	if err != nil {
// 		return fmt.Errorf("invalid log level %q: %w", logLevel, err)
// 	}

// 	backendLeveled.SetLevel(level, "")
// 	logging.SetBackend(backendLeveled)
// 	return nil
// }

// func main() {
// 	v := InitConfig()

// 	if err := InitLogger(v.GetString("log.level")); err != nil {
// 		log.Criticalf("Logger initialization failed: %s", err)
// 		os.Exit(1)
// 	}

// 	// Handle shutdown signals
// 	stop := make(chan os.Signal, 1)
// 	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

// 	// Start request builder
// 	cfg := common.ResponseBuilderConfig{
// 		MiddlewareURL: v.GetString("request_handler.url"),
// 	}
// 	builder := common.NewResponseBuilder(cfg)

// 	go builder.Start()

// 	// Block until signal is received
// 	<-stop

// 	log.Info("Shutdown signal received, cleaning up...")
// 	log.Info("Shutdown complete")
// }
