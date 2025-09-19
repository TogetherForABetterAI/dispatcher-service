package unit

import (
	"testing"

	"github.com/mlops-eval/data-dispatcher-service/src/test/testdata"
	"github.com/stretchr/testify/assert"
)

func TestMNISTDataGeneration(t *testing.T) {
	t.Run("Generate MNIST data with correct dimensions", func(t *testing.T) {
		dataset := testdata.LoadMNISTWithSize(100)

		assert.Equal(t, 100, dataset.Size)
		assert.Equal(t, 100, len(dataset.Images))
		assert.Equal(t, 100, len(dataset.Labels))

		width, height := testdata.GetImageDimensions()
		assert.Equal(t, 28, width)
		assert.Equal(t, 28, height)

		for i, image := range dataset.Images {
			assert.Equal(t, 784, len(image), "Image %d should have 784 bytes", i)

			assert.True(t, dataset.Labels[i] >= 0 && dataset.Labels[i] <= 9,
				"Label %d should be between 0 and 9", dataset.Labels[i])
		}
	})

	t.Run("MNIST batching works correctly", func(t *testing.T) {
		dataset := testdata.LoadMNISTWithSize(250)
		batchSize := 100

		imageData1, labelData1, isLast1 := dataset.GetBatch(batchSize, 0)
		assert.Equal(t, 100*784, len(imageData1)) 
		assert.Equal(t, 100, len(labelData1))    
		assert.False(t, isLast1)

		imageData2, labelData2, isLast2 := dataset.GetBatch(batchSize, 1)
		assert.Equal(t, 100*784, len(imageData2))
		assert.Equal(t, 100, len(labelData2))
		assert.False(t, isLast2)

		imageData3, labelData3, isLast3 := dataset.GetBatch(batchSize, 2)
		assert.Equal(t, 50*784, len(imageData3)) 
		assert.Equal(t, 50, len(labelData3))
		assert.True(t, isLast3)

		imageData4, labelData4, isLast4 := dataset.GetBatch(batchSize, 3)
		assert.Nil(t, imageData4)
		assert.Nil(t, labelData4)
		assert.True(t, isLast4)
	})

	t.Run("Image normalization works", func(t *testing.T) {
		dataset := testdata.LoadMNISTWithSize(1)
		image := dataset.Images[0]

		normalized := testdata.NormalizeImage(image)
		assert.Equal(t, len(image), len(normalized))

		for i, val := range normalized {
			assert.True(t, val >= -1.0 && val <= 1.0,
				"Pixel %d normalized value %f should be in range [-1, 1]", i, val)
		}
	})

	t.Run("Different digit patterns", func(t *testing.T) {
		config := testdata.DefaultMNISTConfig()
		config.NumSamples = 10
		dataset := testdata.GenerateMNISTData(config)

		assert.Equal(t, 10, len(dataset.Images))
		assert.Equal(t, 10, len(dataset.Labels))

		firstImage := dataset.Images[0]
		allIdentical := true
		for i := 1; i < len(dataset.Images); i++ {
			if !bytesEqual(firstImage, dataset.Images[i]) {
				allIdentical = false
				break
			}
		}
		assert.False(t, allIdentical, "Generated images should not all be identical")
	})

	t.Run("Create MNIST batch for protobuf", func(t *testing.T) {
		batch := testdata.CreateMNISTBatch(5, 0, true)

		assert.NotNil(t, batch)
		assert.Equal(t, int32(0), batch.BatchIndex)
		assert.True(t, batch.IsLastBatch)
		assert.Equal(t, 5*784, len(batch.Data)) 
		assert.Equal(t, 5, len(batch.Labels))  

		for i, label := range batch.Labels {
			assert.True(t, label >= 0 && label <= 9,
				"Label %d at position %d should be between 0 and 9", label, i)
		}
	})
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
