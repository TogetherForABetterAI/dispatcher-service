package testdata

import (
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
)

// CreateSampleBatch creates a sample DataBatchLabeled for testing
func CreateSampleBatch(batchIndex int32, isLastBatch bool) *datasetpb.DataBatchLabeled {
	// Create sample data (simulating MNIST-like data)
	// MNIST images are 28x28 = 784 bytes each
	sampleData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9} // Simplified for testing
	sampleLabels := []int32{0, 1, 2}                // Sample labels

	return &datasetpb.DataBatchLabeled{
		Data:        sampleData,
		BatchIndex:  batchIndex,
		IsLastBatch: isLastBatch,
		Labels:      sampleLabels,
	}
}

// CreateMNISTBatch creates a realistic MNIST batch for testing
func CreateMNISTBatch(batchSize int, batchIndex int32, isLastBatch bool) *datasetpb.DataBatchLabeled {
	// Generate MNIST data
	mnist := LoadMNISTForTesting()

	// Get batch data
	imageData, labelData, _ := mnist.GetBatch(batchSize, int(batchIndex))

	return &datasetpb.DataBatchLabeled{
		Data:        imageData,
		BatchIndex:  batchIndex,
		IsLastBatch: isLastBatch,
		Labels:      labelData,
	}
}

// CreateMNISTDataset creates a complete MNIST dataset for testing
func CreateMNISTDataset(totalSamples int) *MNISTDataset {
	return LoadMNISTWithSize(totalSamples)
}

// CreateTestRabbitConfig creates a test RabbitMQ configuration
func CreateTestRabbitConfig() *TestMiddlewareConfig {
	return &TestMiddlewareConfig{
		Host:       "localhost",
		Port:       5672,
		Username:   "test",
		Password:   "test",
		MaxRetries: 3,
	}
}

// TestMiddlewareConfig is a simple config for testing
type TestMiddlewareConfig struct {
	Host       string
	Port       int32
	Username   string
	Password   string
	MaxRetries int
}

// SampleBatch creates a sample batch for testing
func SampleBatch(batchIndex int32, isLastBatch bool) *datasetpb.DataBatchLabeled {
	// Create concatenated data bytes (simulating serialized batch data)
	data := make([]byte, 9) // 3 samples * 3 bytes each
	labels := make([]int32, 3)

	for i := 0; i < 3; i++ {
		// Sample image data (just dummy bytes)
		data[i*3] = byte(i)
		data[i*3+1] = byte(i + 1)
		data[i*3+2] = byte(i + 2)
		labels[i] = int32(i % 10) // Sample labels 0-9
	}

	return &datasetpb.DataBatchLabeled{
		Data:        data,
		BatchIndex:  batchIndex,
		IsLastBatch: isLastBatch,
		Labels:      labels,
	}
}

// SampleBatches creates multiple sample batches for testing
func SampleBatches(count int32) []*datasetpb.DataBatchLabeled {
	batches := make([]*datasetpb.DataBatchLabeled, count)

	for i := int32(0); i < count; i++ {
		isLast := i == count-1
		batches[i] = SampleBatch(i, isLast)
	}

	return batches
}
