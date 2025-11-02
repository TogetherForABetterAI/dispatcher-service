package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	DATASET_EXCHANGE       = "dataset_exchange"
	CONNECTION_QUEUE_NAME  = "data_dispatcher_connections_queue"
	SCALABILITY_QUEUE_NAME = "scalability_queue"
	SCALABILITY_EXCHANGE   = "scalability_exchange"
	REPLICA_NAME           = "dispatcher"
	RESET_CAPACITY = 0.7
)

type Interface interface {
	GetLogLevel() string
	GetReplicaName() string
	GetConsumerTag() string
	GetMiddlewareConfig() *MiddlewareConfig
	GetGrpcConfig() *GrpcConfig
	GetWorkerPoolSize() int
	IsLeader() bool
	GetMinThreshold() int
	GetStartupTimeout() time.Duration
}

type GlobalConfig struct {
	logLevel         string
	replicaName      string
	consumerTag      string
	middlewareConfig *MiddlewareConfig
	grpcConfig       *GrpcConfig
	workerPoolSize   int
	isLeader         bool
	minThreshold     int
	startupTimeout   time.Duration
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

	consumerTag := os.Getenv("CONSUMER_TAG")
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

	// Get worker pool size from environment (optional with default)
	workerPoolSize := 10
	if poolSizeStr := os.Getenv("WORKER_POOL_SIZE"); poolSizeStr != "" {
		parsed, err := strconv.Atoi(poolSizeStr)
		if err != nil {
			return GlobalConfig{}, fmt.Errorf("WORKER_POOL_SIZE must be a valid integer: %w", err)
		}
		workerPoolSize = parsed
	}

	// Get leader status from environment (optional with default)
	isLeader := false
	if leaderStr := os.Getenv("IS_LEADER"); leaderStr != "" {
		isLeader = strings.ToLower(leaderStr) == "true"
	}

	// Get min client threshold from environment (optional with default)
	minThreshold := 4
	if thresholdStr := os.Getenv("MIN_CLIENT_THRESHOLD"); thresholdStr != "" {
		parsed, err := strconv.Atoi(thresholdStr)
		if err != nil {
			return GlobalConfig{}, fmt.Errorf("MIN_CLIENT_THRESHOLD must be a valid integer: %w", err)
		}
		minThreshold = parsed
	}

	// Get startup timeout from environment (optional with default)
	startupTimeoutSeconds := 300 // 5 minutes default
	if timeoutStr := os.Getenv("STARTUP_TIMEOUT_SECONDS"); timeoutStr != "" {
		parsed, err := strconv.Atoi(timeoutStr)
		if err != nil {
			return GlobalConfig{}, fmt.Errorf("STARTUP_TIMEOUT_SECONDS must be a valid integer: %w", err)
		}
		startupTimeoutSeconds = parsed
	}
	startupTimeout := time.Duration(startupTimeoutSeconds) * time.Second

	return GlobalConfig{
		logLevel:       logLevel,
		replicaName:    REPLICA_NAME,
		consumerTag:    consumerTag,
		workerPoolSize: workerPoolSize,
		isLeader:       isLeader,
		minThreshold:   minThreshold,
		startupTimeout: startupTimeout,
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

func (c GlobalConfig) GetReplicaName() string {
	return c.replicaName
}

func (c GlobalConfig) GetConsumerTag() string {
	return c.consumerTag
}

func (c GlobalConfig) GetMiddlewareConfig() *MiddlewareConfig {
	return c.middlewareConfig
}

func (c GlobalConfig) GetGrpcConfig() *GrpcConfig {
	return c.grpcConfig
}

func (c GlobalConfig) GetWorkerPoolSize() int {
	return c.workerPoolSize
}

func (c GlobalConfig) IsLeader() bool {
	return c.isLeader
}

func (c GlobalConfig) GetMinThreshold() int {
	return c.minThreshold
}

func (c GlobalConfig) GetStartupTimeout() time.Duration {
	return c.startupTimeout
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
