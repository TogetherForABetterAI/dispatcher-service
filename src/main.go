package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/rabbitmq"
)

func main() {
	config := loadConfig()
	setupLogging()
	consumer := createConsumer(config)
	startServiceWithGracefulShutdown(consumer, config)
}

func loadConfig() *config.GlobalConfig {
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	return config
}

func setupLogging() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
}

func createConsumer(config *config.GlobalConfig) *rabbitmq.Consumer {
	// Create client processor
	clientProcessor := processor.NewClientDataProcessor()

	// Create RabbitMQ consumer for new client connection notifications
	consumer, err := rabbitmq.NewConsumer(config.MiddlewareConfig, clientProcessor)
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ consumer: %v", err)
	}

	return consumer
}

func startServiceWithGracefulShutdown(consumer *rabbitmq.Consumer, config *config.GlobalConfig) {
	// Channel to listen for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Start consumer in a goroutine
	go func() {
		slog.Info("Starting service",
			"service", config.ServiceConfig.Name,
			"version", config.ServiceConfig.Version)

		if err := consumer.Start(); err != nil {
			log.Fatalf("Failed to start RabbitMQ consumer: %v", err)
		}

		slog.Info("RabbitMQ consumer started successfully")
	}()

	// Wait for interrupt signal
	<-quit
	slog.Info("Shutting down service...")

	// Attempt graceful shutdown
	consumer.Stop()
	slog.Info("Service exited gracefully")
}
