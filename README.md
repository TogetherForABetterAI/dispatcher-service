# Data Dispatcher Service

A gRPC microservice that handles authenticated client notifications and dispatches dataset batches to RabbitMQ queues.

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- **The dataset-generator-service (gRPC server) must be running before starting this microservice.**
- **RabbitMQ server must be accessible for publishing data batches.**

## About This Service

**Data Dispatcher Service** is a gRPC server written in Go that acts as a data distribution hub. Its main responsibilities are:

- **gRPC API:**
  - Exposes a `NotifyNewClient` RPC method that authentication backends can call when a new client is successfully authenticated.
  - Provides health check endpoints for monitoring.

- **Asynchronous Processing:**
  - Upon receiving a new client notification, immediately acknowledges the request and spawns a goroutine to handle data processing.
  - Each client is processed independently with proper context management and graceful shutdown support.

- **Dataset Integration:**
  - Communicates with the `dataset-generator-service` via gRPC to fetch data batches.
  - Supports configurable datasets (MNIST, CIFAR-10, etc.) and batch sizes.

- **RabbitMQ Publishing:**
  - Publishes fetched data batches to the `dataset-exchange` RabbitMQ exchange.
  - Uses routing keys for message routing to specific queues in the exchange.
  - Includes retry logic with exponential backoff for reliable message delivery.
  - Each message contains batch metadata and is marked as persistent.

- **Observability:**
  - Structured JSON logging with configurable log levels.
  - Comprehensive error handling and monitoring capabilities.


## gRPC Interface

### NotifyNewClient

Called by authentication backends when a new client is authenticated:

```protobuf
rpc NotifyNewClient(NewClientRequest) returns (NewClientResponse);

message NewClientRequest {
  string client_id = 1;    // Unique identifier of the client
  string routing_key = 2;  // Routing key used to publish to both exchanges
}

message NewClientResponse {
  string status = 1;       // Status of the operation (e.g., "OK", "ERROR")
  string message = 2;      // Optional message or error description
}
```

**Note**: The service publishes data to the `dataset-exchange` RabbitMQ exchange using the provided `routing_key` for message routing. RabbitMQ connection details are configured via environment variables:
- `RABBITMQ_HOST` (default: localhost)
- `RABBITMQ_PORT` (default: 5672)
- `RABBITMQ_USER` (default: guest)
- `RABBITMQ_PASS` (default: guest)

### Example Usage

```go
// Connect to the service
conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := pb.NewClientNotificationServiceClient(conn)

// Notify about a new client
response, err := client.NotifyNewClient(ctx, &pb.NewClientRequest{
    ClientId:    "user123",
    RoutingKey:  "user123-data-key",
})
```

## Running the Server


### Docker Compose


1. **Build and start the server:**
   ```bash
   sudo docker compose up --build
   ```
   This will build the Docker image and start the server on port 8080.

2. **Stop the server:**
   ```bash
   sudo docker compose down
   ```

## Data Flow

1. **Authentication Backend** calls `NotifyNewClient` with client details and routing key
2. **Data Dispatcher Service** immediately responds with "OK" and spawns a goroutine
3. **Goroutine** connects to the dataset-generator-service and fetches data batches
4. **Each batch** is published to the `dataset-exchange` using the provided routing key
5. **Process continues** until all dataset batches are published or context is cancelled

## RabbitMQ Message Format

Published messages contain protobuf-serialized `DataBatch` messages with the following structure:

```protobuf
message DataBatch {
  bytes data = 1;           // serialized tensor data
  int32 batch_index = 2;    // batch index
  bool is_last_batch = 3;   // indicates if this is the last batch
}
```

**Message Properties:**
- **Content-Type**: `application/x-protobuf`
- **Headers**: Include `client_id`, `batch_index`, and `is_last_batch` for routing and metadata
- **Body**: Protobuf-serialized `DataBatch` message

## Health Monitoring

The service provides health check endpoints:

```bash
# Using grpcurl
grpcurl -plaintext localhost:8080 newClient.ClientNotificationService/HealthCheck
```


