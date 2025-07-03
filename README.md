# Data Dispatcher Service



## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- **The dataset-generator-service (gRPC server) must be running before starting this microservice.**

## About This Service

**Data Dispatcher Service** is a concurrent TCP server written in Go. Its main responsibilities are:

- **Concurrent Client Handling:**
  - The server accepts multiple TCP client connections simultaneously, each handled in its own goroutine for high concurrency and scalability.

- **gRPC Communication:**
  - For each client, the server communicates with the `dataset-generator-service` (a Python gRPC microservice) to fetch data batches on demand.
  - The server acts as a bridge between TCP clients and the gRPC data provider.

- **Batch Streaming:**
  - Every time a client connects, it starts receiving batches of data. (e.g., images from MNIST) sequentially.
  - The server keeps track of the batch index for each client, ensuring clients can progress through the dataset independently.

- **Protocol:**
  - Data batches are serialized as JSON and sent over the TCP connection to the client.
  - The protocol supports signaling the end of the dataset (EOF) to the client.

- **Use Cases:**
  - This service is ideal for scenarios where multiple consumers (e.g., model trainers, data processors) need to stream batches of data concurrently from a central dataset provider.

**Architecture Overview:**
- TCP clients <-> [this service] <-> dataset-generator-service (gRPC Python)

## Running the Server with Docker Compose

1. **For easier proto generation, use the provided script:**

    ```bash
    # Make the script executable (first time only)
    chmod +x src/scripts/generate_proto.sh

    # Run the script to regenerate proto files
    sudo src/scripts/generate_proto.sh
    ```

    This script will:
    - Delete the old generated files in `src/pb/`
    - Create a fresh `src/pb/` directory
    - Run the proto-gen container to regenerate the code

2. **Build and start the server:**
   ```bash
   sudo docker compose up --build data-dispatcher-service
   ```
   This will build the Docker image (if needed) and start the server, exposing it on port 8080.

3. **Stop the server:**
   ```bash
   sudo docker compose down
   ```

## Notes
- If you add or change dependencies, update your `go.mod` and `go.sum` locally, then rebuild the Docker images.
