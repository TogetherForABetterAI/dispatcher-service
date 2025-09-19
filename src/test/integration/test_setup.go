package integration

import (
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/test/mocks"
	"github.com/sirupsen/logrus"
)

func CreateTestProcessor(
	mockDatasetClient *mocks.MockDatasetServiceClient,
	mockPublisher *mocks.MockMiddlewarePublisher,
) *processor.ClientDataProcessor {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	grpcConfig := &config.GrpcConfig{
		DatasetName: "mnist",
		BatchSize:   300,
	}

	return processor.NewClientDataProcessor(
		mockDatasetClient,
		mockPublisher,
		logger,
		grpcConfig,
	)
}

func CreateTestProcessorWithConfig(
	mockDatasetClient *mocks.MockDatasetServiceClient,
	mockPublisher *mocks.MockMiddlewarePublisher,
	config *config.GrpcConfig,
) *processor.ClientDataProcessor {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return processor.NewClientDataProcessor(
		mockDatasetClient,
		mockPublisher,
		logger,
		config,
	)
}
