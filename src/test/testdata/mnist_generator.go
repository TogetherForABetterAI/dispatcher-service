package testdata

import (
	"math"
	"math/rand"
)

// MNISTDataset represents a synthetic MNIST-like dataset for testing
type MNISTDataset struct {
	Images [][]byte // Each image is 28x28 = 784 bytes (grayscale values 0-255)
	Labels []int32  // Each label is an int32 (0-9)
	Size   int      // Total number of samples
}

// MNISTConfig holds configuration for generating MNIST data
type MNISTConfig struct {
	NumSamples int   // Number of samples to generate
	ImageSize  int   // Size of each image (28 for MNIST 28x28)
	NumClasses int   // Number of classes (10 for digits 0-9)
	Seed       int64 // Random seed for reproducible tests
}

// DefaultMNISTConfig returns a default configuration for testing
func DefaultMNISTConfig() *MNISTConfig {
	return &MNISTConfig{
		NumSamples: 1000,
		ImageSize:  28,
		NumClasses: 10,
		Seed:       42, // Fixed seed for reproducible tests
	}
}

// GenerateMNISTData creates synthetic MNIST-like data for testing
func GenerateMNISTData(config *MNISTConfig) *MNISTDataset {
	rand.Seed(config.Seed)

	dataset := &MNISTDataset{
		Images: make([][]byte, config.NumSamples),
		Labels: make([]int32, config.NumSamples),
		Size:   config.NumSamples,
	}

	for i := 0; i < config.NumSamples; i++ {
		label := int32(rand.Intn(config.NumClasses))
		dataset.Labels[i] = label

		dataset.Images[i] = generateDigitLikeImage(int(label), config.ImageSize)
	}

	return dataset
}

// generateDigitLikeImage creates a synthetic 28x28 image that somewhat resembles a digit
func generateDigitLikeImage(digit int, size int) []byte {
	image := make([]byte, size*size)
	centerX, centerY := size/2, size/2

	// Create different patterns for different digits
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			idx := y*size + x

			// Distance from center
			dx := float64(x - centerX)
			dy := float64(y - centerY)
			dist := math.Sqrt(dx*dx + dy*dy)

			var intensity byte = 0

			switch digit {
			case 0: // Circle
				if dist > 6 && dist < 10 {
					intensity = byte(255 - int(dist*10)) // Gradient effect
				}
			case 1: // Vertical line
				if x >= centerX-1 && x <= centerX+1 {
					intensity = 200
				}
			case 2: // Horizontal lines with some curves
				if (y < size/3 && dist < 8) || (y > 2*size/3 && dist < 8) ||
					(y >= size/3 && y <= 2*size/3 && x < size/3) {
					intensity = 180
				}
			case 3: // Two horizontal segments
				if (y < size/3 || y > 2*size/3) && x > centerX {
					intensity = 190
				} else if y >= size/3 && y <= 2*size/3 && x > centerX {
					intensity = 160
				}
			case 4: // L-shaped pattern
				if (x < centerX && y > centerY) || (y >= centerY-1 && y <= centerY+1) {
					intensity = 210
				}
			case 5: // S-like pattern
				if (y < size/3) || (y >= size/3 && y <= 2*size/3 && x < centerX) ||
					(y > 2*size/3 && x > centerX) {
					intensity = 175
				}
			default: // For digits 6-9, just use a circular pattern with variations
				if dist > 4 && dist < 12 {
					intensity = byte(150 + (digit-6)*20)
				}
			}

			// Add some noise for realism
			noise := rand.Intn(50) - 25
			finalIntensity := int(intensity) + noise
			if finalIntensity < 0 {
				finalIntensity = 0
			} else if finalIntensity > 255 {
				finalIntensity = 255
			}

			image[idx] = byte(finalIntensity)
		}
	}

	return image
}

// GetBatch returns a batch of data similar to how the real dataset service would
func (m *MNISTDataset) GetBatch(batchSize, batchIndex int) ([]byte, []int32, bool) {
	start := batchIndex * batchSize
	end := start + batchSize

	if start >= m.Size {
		return nil, nil, true // No more data
	}

	if end > m.Size {
		end = m.Size
	}

	isLastBatch := end >= m.Size

	// Serialize images into a single byte array (similar to protobuf format)
	var imageData []byte
	var labelData []int32

	for i := start; i < end; i++ {
		imageData = append(imageData, m.Images[i]...)
		labelData = append(labelData, m.Labels[i])
	}

	return imageData, labelData, isLastBatch
}

// SerializeForProtobuf converts the batch data to the format expected by protobuf
func SerializeBatchForProtobuf(imageData []byte, labelData []int32, batchIndex int32, isLastBatch bool) ([]byte, []int32) {
	// For simplicity, we'll just return the raw data
	// In a real scenario, you might want to add headers or structure the data differently
	return imageData, labelData
}

// LoadMNISTForTesting is the equivalent of your Python load_mnist function
func LoadMNISTForTesting() *MNISTDataset {
	config := DefaultMNISTConfig()
	return GenerateMNISTData(config)
}

// LoadMNISTWithSize allows specifying the dataset size
func LoadMNISTWithSize(numSamples int) *MNISTDataset {
	config := DefaultMNISTConfig()
	config.NumSamples = numSamples
	return GenerateMNISTData(config)
}

// GetImageDimensions returns the dimensions of MNIST images
func GetImageDimensions() (width, height int) {
	return 28, 28
}

// NormalizeImage applies normalization similar to the Python transforms
// Converts to range [-1, 1] similar to transforms.Normalize((0.5,), (0.5,))
func NormalizeImage(image []byte) []float32 {
	normalized := make([]float32, len(image))
	for i, pixel := range image {
		// Convert from [0, 255] to [0, 1] then to [-1, 1]
		normalized[i] = (float32(pixel)/255.0 - 0.5) / 0.5
	}
	return normalized
}
