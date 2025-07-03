package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type BatchMessage struct {
	BatchIndex int           `json:"batch_index"`
	BatchData  []interface{} `json:"batch_data"`
	ClientID   string        `json:"client_id"`
	EOF        bool          `json:"EOF"`
}

func main() {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to server. Receiving batches...")
	startTime := time.Now()

	reader := bufio.NewReader(conn)
	batchCount := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading from connection: %v\n", err)
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse the JSON message
		var batchMsg BatchMessage
		if err := json.Unmarshal([]byte(line), &batchMsg); err != nil {
			fmt.Printf("Error parsing JSON: %v\n", err)
			continue
		}

		batchCount++
		currentTime := time.Now()
		// Report progress every 100 batches
		if batchCount%100 == 0 {
			elapsed := currentTime.Sub(startTime)
			rate := float64(batchCount) / elapsed.Seconds()
			fmt.Printf("Received %d batches in %.1fs (%.2f batches/sec)\n", batchCount, elapsed.Seconds(), rate)
		}

		// Check if this is the last batch
		if batchMsg.EOF {
			totalTime := currentTime.Sub(startTime)
			totalRate := float64(batchCount) / totalTime.Seconds()
			fmt.Printf("\n🎉 COMPLETED! Received EOF signal.\n")
			fmt.Printf("📊 Total batches received: %d\n", batchCount)
			fmt.Printf("⏱️  Total time: %.2f seconds\n", totalTime.Seconds())
			fmt.Printf("🚀 Average rate: %.2f batches/second\n", totalRate)
			break
		}
	}

	if batchCount == 0 {
		fmt.Println("No batches received.")
	}
}
