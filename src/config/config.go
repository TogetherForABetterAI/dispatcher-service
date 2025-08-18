package config

import (
	"os"
	"strconv"
)

type GlobalConfig struct {
	MiddlewareConfig *MiddlewareConfig
	GrpcConfig       *GrpcConfig
}

type GrpcConfig struct {
	DatasetAddr string
	DatasetName string
	BatchSize   int32
}
// Config holds RabbitMQ connection configuration
type MiddlewareConfig struct {
	Host     string
	Port     int32
	Username string
	Password string
	MaxRetries int
}

func InitializeConfig() *GlobalConfig {
	// Get RabbitMQ connection details from environment
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	if rabbitHost == "" {
		rabbitHost = "localhost"
	}

	rabbitPort := int32(5672) // default RabbitMQ port
	if portStr := os.Getenv("RABBITMQ_PORT"); portStr != "" {
		if parsed, err := strconv.ParseInt(portStr, 10, 32); err == nil {
			rabbitPort = int32(parsed)
		}
	}

	rabbitUser := os.Getenv("RABBITMQ_USER")
	if rabbitUser == "" {
		rabbitUser = "guest"
	}

	rabbitPass := os.Getenv("RABBITMQ_PASS")
	if rabbitPass == "" {
		rabbitPass = "guest"
	}

	// Get max retries from environment or use default
	maxRetries := 3
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		if parsed, err := strconv.Atoi(retriesStr); err == nil {
			maxRetries = parsed
		}
	}

	// Get dataset service address from environment or use default
	datasetAddr := os.Getenv("DATASET_SERVICE_ADDR")
	if datasetAddr == "" {
		datasetAddr = "dataset-grpc-service:50051"
	}

	// Get dataset configuration from environment or use defaults
	datasetName := os.Getenv("DATASET_NAME")
	if datasetName == "" {
		datasetName = "mnist"
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	batchSize := int32(300)
	if batchSizeStr != "" {
		if parsed, err := strconv.ParseInt(batchSizeStr, 10, 32); err == nil {
			batchSize = int32(parsed)
		}
	}
	
	middlewareConfig := MiddlewareConfig{
		Host:     rabbitHost,
		Port:     rabbitPort,
		Username: rabbitUser,
		Password: rabbitPass,
		MaxRetries: maxRetries,
	}
	grpcConfig := GrpcConfig{
		DatasetAddr: datasetAddr,
		DatasetName: datasetName,
		BatchSize:   batchSize,
	}

	return &GlobalConfig{
		MiddlewareConfig: &middlewareConfig,
		GrpcConfig:       &grpcConfig,
	}
}

const(
	DATASET_EXCHANGE = "dataset-exchange"
)