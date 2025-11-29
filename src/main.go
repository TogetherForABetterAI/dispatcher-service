package main

import (
	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/server"
	"log"
	"log/slog"
	"os"
)

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

func main() {
	config := loadConfig()
	setupLogging(config)

	srv, err := server.NewServer(config)
	if err != nil {
		slog.Error("Failed to initialize server", "error", err)
		os.Exit(1)
	}

	if err := srv.Run(); err != nil {
		slog.Error("Service exited with error", "error", err)
		os.Exit(1)
	}

	slog.Info("Service shutdown complete. Exiting.")
}
