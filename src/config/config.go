package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

const (
	DATASET_EXCHANGE      = "dataset_exchange"
	CONNECTION_QUEUE_NAME = "data_dispatcher_connections_queue"
)

type GlobalConfig struct {
	logLevel         string
	serviceName      string
	containerName    string
	middlewareConfig *MiddlewareConfig
	grpcConfig       *GrpcConfig
}

type GrpcConfig struct {
	datasetServiceAddr string
	batchSize          int32
}

// MiddlewareConfig holds RabbitMQ connection configuration
type MiddlewareConfig struct {
	host       string
	port       int32
	username   string
	password   string
	maxRetries int
}

func NewConfig() (GlobalConfig, error) {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		return GlobalConfig{}, fmt.Errorf("failed to load .env file: %w", err)
	}

	// Get RabbitMQ connection details from environment
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	if rabbitHost == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_HOST environment variable is required")
	}

	rabbitPortStr := os.Getenv("RABBITMQ_PORT")
	if rabbitPortStr == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PORT environment variable is required")
	}
	rabbitPort, err := strconv.ParseInt(rabbitPortStr, 10, 32)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PORT must be a valid integer: %w", err)
	}

	rabbitUser := os.Getenv("RABBITMQ_USER")
	if rabbitUser == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_USER environment variable is required")
	}

	rabbitPass := os.Getenv("RABBITMQ_PASS")
	if rabbitPass == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PASS environment variable is required")
	}

	// Set log level from environment
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		return GlobalConfig{}, fmt.Errorf("LOG_LEVEL environment variable is required")
	}

	// Get dataset service address from environment
	datasetAddr := os.Getenv("DATASET_SERVICE_ADDR")
	if datasetAddr == "" {
		return GlobalConfig{}, fmt.Errorf("DATASET_SERVICE_ADDR environment variable is required")
	}

	// Get batch size from environment
	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return GlobalConfig{}, fmt.Errorf("BATCH_SIZE environment variable is required")
	}
	batchSize, err := strconv.ParseInt(batchSizeStr, 10, 32)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("BATCH_SIZE must be a valid integer: %w", err)
	}

	// Get max retries from environment (optional with default)
	maxRetries := 3
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		parsed, err := strconv.Atoi(retriesStr)
		if err != nil {
			return GlobalConfig{}, fmt.Errorf("MAX_RETRIES must be a valid integer: %w", err)
		}
		maxRetries = parsed
	}

	// Get container name from hostname (automatic detection)
	containerName, err := os.Hostname()
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("failed to get container hostname: %w", err)
	}

	return GlobalConfig{
		logLevel:      logLevel,
		serviceName:   "data-dispatcher-service",
		containerName: containerName,
		middlewareConfig: &MiddlewareConfig{
			host:       rabbitHost,
			port:       int32(rabbitPort),
			username:   rabbitUser,
			password:   rabbitPass,
			maxRetries: maxRetries,
		},
		grpcConfig: &GrpcConfig{
			datasetServiceAddr: datasetAddr,
			batchSize:          int32(batchSize),
		},
	}, nil
}


// GlobalConfig getters
func (c GlobalConfig) GetLogLevel() string {
	return c.logLevel
}

func (c GlobalConfig) GetServiceName() string {
	return c.serviceName
}

func (c GlobalConfig) GetContainerName() string {
	return c.containerName
}

func (c GlobalConfig) GetMiddlewareConfig() *MiddlewareConfig {
	return c.middlewareConfig
}

func (c GlobalConfig) GetGrpcConfig() *GrpcConfig {
	return c.grpcConfig
}

// MiddlewareConfig getters
func (m MiddlewareConfig) GetHost() string {
	return m.host
}

func (m MiddlewareConfig) GetPort() int32 {
	return m.port
}

func (m MiddlewareConfig) GetUsername() string {
	return m.username
}

func (m MiddlewareConfig) GetPassword() string {
	return m.password
}

func (m MiddlewareConfig) GetMaxRetries() int {
	return m.maxRetries
}

// GrpcConfig getters
func (g GrpcConfig) GetDatasetServiceAddr() string {
	return g.datasetServiceAddr
}

func (g GrpcConfig) GetBatchSize() int32 {
	return g.batchSize
}
