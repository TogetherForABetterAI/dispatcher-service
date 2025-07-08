package grpc

import (
	"context"

	pb "github.com/mlops-eval/data-dispatcher-service/src/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.DatasetServiceClient
}

func NewClient(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}
	client := pb.NewDatasetServiceClient(conn)
	return &Client{conn: conn, client: client}, nil
}

func (c *Client) GetBatch(ctx context.Context, req *pb.GetBatchRequest) (*pb.DataBatch, error) {
	return c.client.GetBatch(ctx, req)
}
