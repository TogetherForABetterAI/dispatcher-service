package grpc

import (
	"context"
	"fmt"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a gRPC client for the dataset service
type Client struct {
	conn   *grpc.ClientConn
	client datasetpb.DatasetServiceClient
}

// NewClient creates a new gRPC client for the dataset service
func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to dataset service: %w", err)
	}

	client := datasetpb.NewDatasetServiceClient(conn)

	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

// GetBatch fetches a single batch from the dataset service
func (c *Client) GetBatch(ctx context.Context, req *datasetpb.GetBatchRequest) (*datasetpb.DataBatchLabeled, error) {
	return c.client.GetBatch(ctx, req)
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	return c.conn.Close()
}
