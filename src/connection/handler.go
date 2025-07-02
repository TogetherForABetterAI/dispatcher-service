package connection

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/pb"
	"github.com/mlops-eval/data-dispatcher-service/src/protocol"
)

func init() {
	godotenv.Load()
}

func Handle(conn net.Conn, clientID string) {
	defer conn.Close()
	batch_index := 0
	grpcClient, err := grpc.NewClient(os.Getenv("GRPC_SERVER_ADDRESS"))

	if err != nil {
		//deberia intentar conectarme nuevamente
		// al server gRPC me imagino
		log.Printf("error creating grpc client: %v", err)
		return
	}

	for batchID := 0; ; batchID++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req := &pb.GetBatchRequest{
			DatasetName: "mnist",
			BatchSize:   2,
			BatchIndex: int32(batch_index),
		}
		batch, err := grpcClient.GetBatch(ctx, req)
		cancel()
		if err != nil {
			log.Printf("error fetching batch for client %s: %v", clientID, err)
			return
		}

		batchMsg := &protocol.BatchMessage{
			BatchID:   int(batch.GetBatchId()),
			BatchData: []interface{}{batch.GetData()},
			ClientID:  clientID,
			EOF:       batch.GetIsLastBatch(),
		}

		if err := protocol.EncodeBatchMessage(conn, batchMsg); err != nil {
			log.Printf("error sending batch to client %s: %v", clientID, err)
			return
		}

		if batch.GetIsLastBatch() {
			log.Printf("transmission to client %s completed", clientID)
			return
		}

		batch_index++
	}
}
