// Package grpc Handles how the gRPC requests communicates with out ledger backend.
package grpc

import (
	"context"
	"fmt"
	"net"

	"concurrent-bank-ledger/internal/domain"
	
	// Import the gRPC code that protoc just generated for you...
	pb "concurrent-bank-ledger/api/proto/ledgerapi"

	"google.golang.org/grpc"
)

// Server represents our gRPC adapter...
type Server struct {
	pb.UnimplementedLedgerServiceServer
	Ledger *domain.Ledger
}

// Transfer implements the exact gRPC method we defined in our ledger.proto file.
func (s *Server) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	// 1. Extract values directly from request object...
	source, target := req.GetSource(), req.GetTarget()
	amount, key := req.GetAmount(), req.GetIdempotencyKey()

	// Feed them straight through...
	success, err := s.Ledger.Transfer(source, target, amount, key)
	if err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if !success {
		return &pb.TransferResponse{
			Success: false,
			Message: "Transfer rejected by ledger...",
		}, nil
	}
	
	return &pb.TransferResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully transferred %d from %s to %s!", amount, source, target),
	}, nil
}

// StartGRPCServer is a helper function to bind our server.
func StartGRPCServer(port string, domainLedger *domain.Ledger) error {
	// Open a standard TCP network port listener...
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	// Create a new & empty gRPC server instance...
	gRPCServer := grpc.NewServer()
	
	// Instantiate our custom Server struct with the domain ledger...
	srv := &Server{
		Ledger: domainLedger,
	}

	// Register our implementation with the gRPC server router...
	pb.RegisterLedgerServiceServer(gRPCServer, srv)
	fmt.Printf("gRPC Banking Ledger Server is running securely on port %s...\n", port)
	
	return gRPCServer.Serve(listener)
}
