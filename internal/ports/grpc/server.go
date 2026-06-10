// Package grpc Handles how the gRPC requests communicates with out ledger backend.
package grpc

import (
	"context"
	"fmt"
	"net"

	"concurrent-bank-ledger/internal/domain"

	// Import the gRPC code that protoc just generated for you...
	pb "concurrent-bank-ledger/api/proto/ledgerapi"

	g "google.golang.org/grpc"
)

// Server represents our gRPC adapter...
type Server struct {
	pb.UnimplementedLedgerServiceServer
	Queue chan domain.LedgerCommand
}

// Transfer implements the exact gRPC method we defined in our ledger.proto file.
func (s *Server) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	// 1. Create a reply channel..
	replyChan := make(chan domain.TransferResponse, 1)

	// 2. Build the structural Request payload slip.
	transferReq := &domain.TransferRequest{
		Source: req.GetSource(),
		Target: req.GetTarget(),
		Amount: req.GetAmount(),
		IdempotencyKey: req.GetIdempotencyKey(),
		ReplyTo: replyChan,
	}
	
	// 3. Slip queue inside a generic LedgerCommand and drop it down the channel.
	s.Queue <- domain.LedgerCommand{
		Type: "TRANSFER",
		Transfer: transferReq,
	}

	// 4. Freeze here! blocks completely until the background worker thread gives us back the reply...
	res := <-replyChan

	if res.Err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: res.Err.Error(),
		}, nil
	}

	if !res.Success {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("Transfer rejected by core engine... %s -> %s", req.GetSource(), req.GetTarget()),
		}, nil
	}

	return &pb.TransferResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully transferred %d from %s to %s!", req.GetAmount(), req.GetSource(), req.GetTarget()),
	}, nil
}

func (s *Server) InitialiseAccount(ctx context.Context, req *pb.InitialiseAccountRequest) (*pb.InitialiseAccountResponse, error) {
	// 1. Allocate response channel...
	replyChan := make(chan error, 1)

	initReq := &domain.InitialiseAccountRequest{
		Username: req.GetUsername(),
		StartingBalance: req.GetBalance(),
		ReplyTo: replyChan,
	}

	// 2. Slip queue into generic LedgerCommand.
	s.Queue <- domain.LedgerCommand{
		Type: "INITIALISE",
		InitAccount: initReq,
	}

	// 3. Freeze...
	err := <-replyChan
	if err != nil {
		return &pb.InitialiseAccountResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &pb.InitialiseAccountResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully initialised account %s with %d starting balance!", req.GetUsername(), req.GetBalance()),
	}, nil
}

// StartGRPCServer is a helper function to bind our server.
func StartGRPCServer(port string, queue chan domain.LedgerCommand) (*g.Server, net.Listener, error) {
	// Open a standard TCP network port listener...
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, nil, err
	}

	// Create a new & empty gRPC server instance...
	gRPCServer := g.NewServer()

	// Instantiate our custom Server struct with the domain ledger...
	srv := &Server{
		Queue: queue,
	}

	// Register our implementation with the gRPC server router...
	pb.RegisterLedgerServiceServer(gRPCServer, srv)

	return gRPCServer, listener, nil
}
