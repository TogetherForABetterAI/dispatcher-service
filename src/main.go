package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/server"
)

func main() {
	config := loadConfig()
	setupLogging(config)

	srv, err := server.NewServer(config)
	if err != nil {
		slog.Error("Failed to initialize server", "error", err)
		os.Exit(1)
	}
	startServiceWithGracefulShutdown(srv, config)

	slog.Info("Service shutdown complete. Exiting.")
}

func loadConfig() config.GlobalConfig {
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	return config
}

func setupLogging(config config.GlobalConfig) {
	logLevel := slog.LevelInfo
	switch config.GetLogLevel() {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)
}

// startServiceWithGracefulShutdown orchestrates the service lifecycle and handles graceful shutdown.
func startServiceWithGracefulShutdown(srv *server.Server, config config.GlobalConfig) {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	// Channel for fatal server errors (crashes)
	serverDone := make(chan error, 1)
	go func() {
		slog.Info("Starting service",
			"service", config.GetConsumerTag(),
			"is_leader", config.IsLeader(),
			"min_threshold", config.GetMinThreshold())
		err := srv.Start()
		serverDone <- err // Notify main when finished
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			slog.Error("Service stopped unexpectedly due to an error", "error", err)
			srv.Stop()
			os.Exit(1)
		}
		slog.Warn("Service stopped unexpectedly without an error.")
	case <-osSignals:
		slog.Info("Received OS signal. Initiating graceful shutdown...")
		srv.Stop()
		<-serverDone
		slog.Info("Service exited gracefully after receiving OS signal.")
	case <-srv.ShutdownRequestChannel():
		slog.Info("Received internal scale-in request. Initiating graceful shutdown...")
		srv.Stop()
		<-serverDone
		slog.Info("Service exited gracefully after internal scale-in request.")
	}
}
