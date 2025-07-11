package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// DataDispatcherServer implements the ClientNotificationService gRPC service
type DataDispatcherServer struct {
	clientpb.UnimplementedClientNotificationServiceServer
	logger     *logrus.Logger
	clientWg   sync.WaitGroup  // Track active client goroutines
	shutdown   chan struct{}   // Signal for graceful shutdown
	processor  ClientProcessor // Interface for processing client data
	grpcServer *grpc.Server    // gRPC server instance
}

// ClientProcessor defines the interface for processing client data
type ClientProcessor interface {
	ProcessClient(ctx context.Context, req *clientpb.NewClientRequest) error
}

// NewDataDispatcherServer creates a new instance of the data dispatcher gRPC server
func NewDataDispatcherServer(processor ClientProcessor) *DataDispatcherServer {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	return &DataDispatcherServer{
		logger:    logger,
		shutdown:  make(chan struct{}),
		processor: processor,
	}
}

// NotifyNewClient handles new client notifications and spawns a goroutine to process them
func (s *DataDispatcherServer) NotifyNewClient(ctx context.Context, req *clientpb.NewClientRequest) (*clientpb.NewClientResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"client_id":   req.ClientId,
		"routing_key": req.RoutingKey,
	}).Info("Received new client notification")

	// Validate request
	if req.ClientId == "" {
		return &clientpb.NewClientResponse{
			Status:  "ERROR",
			Message: "client_id is required",
		}, nil
	}

	if req.RoutingKey == "" {
		return &clientpb.NewClientResponse{
			Status:  "ERROR",
			Message: "routing_key is required",
		}, nil
	}

	// Spawn goroutine to handle client processing
	s.clientWg.Add(1)
	go func() {
		defer s.clientWg.Done()

		// Create a context that can be cancelled on shutdown
		clientCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Listen for shutdown signal
		go func() {
			select {
			case <-s.shutdown:
				cancel()
			case <-clientCtx.Done():
			}
		}()

		if err := s.processor.ProcessClient(clientCtx, req); err != nil {
			s.logger.WithFields(logrus.Fields{
				"client_id": req.ClientId,
				"error":     err.Error(),
			}).Error("Failed to process client")
		} else {
			s.logger.WithFields(logrus.Fields{
				"client_id": req.ClientId,
			}).Info("Successfully completed client processing")
		}
	}()

	return &clientpb.NewClientResponse{
		Status:  "OK",
		Message: "Client processing started",
	}, nil
}

// HealthCheck implements health checking
func (s *DataDispatcherServer) HealthCheck(ctx context.Context, req *clientpb.HealthCheckRequest) (*clientpb.HealthCheckResponse, error) {
	return &clientpb.HealthCheckResponse{
		Status:  "SERVING",
		Message: "Data dispatcher service is healthy",
	}, nil
}

// Start starts the gRPC server
func (s *DataDispatcherServer) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	s.grpcServer = grpc.NewServer()

	// Register the client notification service
	clientpb.RegisterClientNotificationServiceServer(s.grpcServer, s)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	s.logger.WithField("port", port).Info("gRPC server started successfully")

	if err := s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}

	return nil
}

// Stop gracefully stops the server and waits for all client goroutines to finish
func (s *DataDispatcherServer) Stop() {
	s.logger.Info("Initiating graceful shutdown")

	// Signal all client goroutines to stop
	close(s.shutdown)

	// Gracefully stop the gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Wait for all client goroutines to finish
	s.clientWg.Wait()

	s.logger.Info("Graceful shutdown completed")
}
