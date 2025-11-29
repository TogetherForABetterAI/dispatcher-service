package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

const (
	CONNECTION_QUEUE_NAME           = "dispatcher_connection_queue"
	DATASET_EXCHANGE                = "dataset_exchange"
	BATCHES_TO_FETCH                = 10
	DISPATCHER_EXCHANGE             = "dispatcher_exchange"
	DISPATCHER_TO_CLIENT_QUEUE      = "%s_dispatcher_queue"
	DISPATCHER_TO_CALIBRATION_QUEUE = "%s_inputs_cal_queue"
)

type Interface interface {
	GetLogLevel() string
	GetPodName() string
	GetMiddlewareConfig() *MiddlewareConfig
	GetDatabaseConfig() *DatabaseConfig
	GetWorkerPoolSize() int
}

type GlobalConfig struct {
	logLevel         string
	podName          string
	middlewareConfig *MiddlewareConfig
	databaseConfig   *DatabaseConfig
	workerPoolSize   int
}

// DatabaseConfig holds PostgreSQL connection configuration
type DatabaseConfig struct {
	host     string
	port     int32
	user     string
	password string
	dbname   string
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
	// Load environment variables from .env file (optional, for local development)
	// In production (Kubernetes), variables are injected directly via ConfigMap/Secret
	_ = godotenv.Load() // Ignore error if .env file doesn't exist

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

	// Get Pod name from GKE (for consumer tag)
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		// Fallback for local development
		podName = "local-dispatcher"
	}

	// Get PostgreSQL connection details from environment
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		// Default to localhost for Cloud SQL Proxy
		dbHost = "127.0.0.1"
	}

	dbPortStr := os.Getenv("POSTGRES_PORT")
	if dbPortStr == "" {
		dbPortStr = "5432" // Default PostgreSQL port
	}
	dbPort, err := strconv.ParseInt(dbPortStr, 10, 32)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("POSTGRES_PORT must be a valid integer: %w", err)
	}

	dbUser := os.Getenv("POSTGRES_USER")
	if dbUser == "" {
		return GlobalConfig{}, fmt.Errorf("POSTGRES_USER environment variable is required")
	}

	dbPass := os.Getenv("POSTGRES_PASS")
	if dbPass == "" {
		return GlobalConfig{}, fmt.Errorf("POSTGRES_PASS environment variable is required")
	}

	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		return GlobalConfig{}, fmt.Errorf("POSTGRES_DB environment variable is required")
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

	return GlobalConfig{
		logLevel:       logLevel,
		podName:        podName,
		workerPoolSize: workerPoolSize,
		middlewareConfig: &MiddlewareConfig{
			host:       rabbitHost,
			port:       int32(rabbitPort),
			username:   rabbitUser,
			password:   rabbitPass,
			maxRetries: maxRetries,
		},
		databaseConfig: &DatabaseConfig{
			host:     dbHost,
			port:     int32(dbPort),
			user:     dbUser,
			password: dbPass,
			dbname:   dbName,
		},
	}, nil
}

// GlobalConfig getters
func (c GlobalConfig) GetLogLevel() string {
	return c.logLevel
}

func (c GlobalConfig) GetPodName() string {
	return c.podName
}

func (c GlobalConfig) GetMiddlewareConfig() *MiddlewareConfig {
	return c.middlewareConfig
}

func (c GlobalConfig) GetDatabaseConfig() *DatabaseConfig {
	return c.databaseConfig
}

func (c GlobalConfig) GetWorkerPoolSize() int {
	return c.workerPoolSize
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

// DatabaseConfig getters
func (d DatabaseConfig) GetHost() string {
	return d.host
}

func (d DatabaseConfig) GetPort() int32 {
	return d.port
}

func (d DatabaseConfig) GetUser() string {
	return d.user
}

func (d DatabaseConfig) GetPassword() string {
	return d.password
}

func (d DatabaseConfig) GetDBName() string {
	return d.dbname
}
