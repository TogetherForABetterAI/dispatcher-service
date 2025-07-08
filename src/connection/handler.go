package connection

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/joho/godotenv"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/pb"
	"github.com/mlops-eval/data-dispatcher-service/src/protocol"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"

)

func init() {
	godotenv.Load()
}

func Handle(conn net.Conn, clientID string, middleware *middleware.Middleware) {
	defer conn.Close()
	batch_index := 0
	address := "dataset-grpc-service:50051"
	log.Printf("address vale: %v", address)
	grpcClient, err := grpc.NewClient(address)

	if err != nil {
		//deberia intentar conectarme nuevamente
		// al server gRPC me imagino
		log.Printf("error creating grpc client: %v", err)
		return
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req := &pb.GetBatchRequest{
			DatasetName: "mnist",
			BatchSize: 30,
			BatchIndex:  int32(batch_index),
		}
		batch, err := grpcClient.GetBatch(ctx, req)
		cancel()
		if err != nil {
			log.Printf("error fetching batch for client %s: %v", clientID, err)
			return
		}

		batch_bytes, err := protocol.EncodeBatchMessage(batch); 
		if err != nil {
			log.Printf("error sending batch to client %s: %v", clientID, err)
			return
		}
	
		middleware.BasicSend(
			"data",
			batch_bytes,
			"data_exchange",
		)
		middleware.BasicSend(
			"data",
			batch_bytes,
			"calibration_exchange",
		)

		if batch.GetIsLastBatch() {
			log.Printf("transmission to client %s completed", clientID)
			return
		}

		batch_index++

		print("Batch sent to client with index: ", batch_index, "\n")
	}
}
