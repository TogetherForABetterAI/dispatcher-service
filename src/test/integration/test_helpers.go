package integration

import (
	"testing"

	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/test/mocks"
)

type TestSetup struct {
	MockMiddleware    *mocks.MockMiddlewarePublisher
	MockDatasetClient *mocks.MockDatasetServiceClient
	Processor         *processor.ClientDataProcessor
}

func NewTestSetup() *TestSetup {
	setup := &TestSetup{
		MockMiddleware:    &mocks.MockMiddlewarePublisher{},
		MockDatasetClient: &mocks.MockDatasetServiceClient{},
	}

	setup.MockMiddleware.On("Close").Return()

	setup.Processor = CreateTestProcessor(setup.MockDatasetClient, setup.MockMiddleware)

	return setup
}

func (s *TestSetup) AssertAllExpectations(t *testing.T) {
	s.MockMiddleware.AssertExpectations(t)
	s.MockDatasetClient.AssertExpectations(t)
}
