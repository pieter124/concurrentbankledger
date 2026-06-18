// Package grpc Handles how the gRPC requests communicates with out ledger backend.
package grpc

import (
	"context"
	"fmt"
	"net"
	"hash/fnv"
	"concurrent-bank-ledger/internal/domain"

	// Import the gRPC code that protoc just generated for you...
	pb "concurrent-bank-ledger/api/proto/ledgerapi"

	g "google.golang.org/grpc"
)

// Server represents our gRPC adapter...
type Server struct {
	pb.UnimplementedLedgerServiceServer
	Queues []chan domain.LedgerCommand
	WAL domain.WAL
}

func getActorIndex(key string, NoOfActors int) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))

	return int(hasher.Sum32()) % NoOfActors
} 

func sendTransfer(q chan domain.LedgerCommand, source, target string, amount int64, key string) (bool, error) {
	reply := make(chan domain.TransferResponse, 1)
	q <- domain.LedgerCommand{Type: domain.TransferCommand, Transfer: &domain.TransferRequest{
		Source: source, Target: target, Amount: amount, IdempotencyKey: key, ReplyTo: reply,
	}}
	r := <-reply
	return r.Success, r.Err
}

func sendReserve(q chan domain.LedgerCommand, source, target string, amount int64, key string) domain.ReserveResponse {
	reply := make(chan domain.ReserveResponse, 1)
	q <- domain.LedgerCommand{Type: domain.ReserveCommand, Reserve: &domain.ReserveRequest{
		Source: source, Target: target, Amount: amount, IdempotencyKey: key, ReplyTo: reply,
	}}
	return <-reply
}

func sendCredit(q chan domain.LedgerCommand, target string, amount int64, txnID string) (bool, error) {
	reply := make(chan domain.TransferResponse, 1)
	q <- domain.LedgerCommand{Type: domain.CreditCommand, Credit: &domain.CreditRequest{
		Target: target, Amount: amount, TxnID: txnID, ReplyTo: reply,
	}}
	r := <-reply
	return r.Success, r.Err
}

// sendKeyed handles both refund and finalize — both only carry a key.
func sendKeyed(q chan domain.LedgerCommand, cmdType int, key string) {
	reply := make(chan domain.TransferResponse, 1)
	kr := &domain.KeyedRequest{IdempotencyKey: key, ReplyTo: reply}
	cmd := domain.LedgerCommand{Type: cmdType}
	if cmdType == domain.RefundCommand {
		cmd.Refund = kr
	} else {
		cmd.Finalize = kr
	}
	q <- cmd
	<-reply
}

func (s *Server) routeTransfer(source, target string, amount int64, key string) (bool, error) {
	n := len(s.Queues)
	srcIdx := getActorIndex(source, n)
	tgtIdx := getActorIndex(target, n)

	// Same shard case: one actor owns both accounts.
	if srcIdx == tgtIdx {
		return sendTransfer(s.Queues[srcIdx], source, target, amount, key)
	}

	// Cross-shard: hold on source, deliver on target, then mark done (or undo).
	res := sendReserve(s.Queues[srcIdx], source, target, amount, key)
	switch {
	case res.Err != nil:
		return false, res.Err
	case res.AlreadyDone: // completed retry — money already moved, don't credit again
		return true, nil
	case !res.Proceed: // rejected (bad amount / unknown source)
		return false, nil
	}

	ok, err := sendCredit(s.Queues[tgtIdx], target, amount, res.TxnID)
	if err != nil || !ok {
		sendKeyed(s.Queues[srcIdx], domain.RefundCommand, key) // credit failed -> return held money
		return false, err
	}

	sendKeyed(s.Queues[srcIdx], domain.FinalizeCommand, key) // credit landed -> pending becomes success
	return true, nil
}

// Transfer implements the exact gRPC method we defined in our ledger.proto file.
func (s *Server) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResponse, error) {
	ok, err := s.routeTransfer(req.GetSource(), req.GetTarget(), req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: err.Error()}, nil
	}
	if !ok {
		return &pb.TransferResponse{Success: false,
			Message: fmt.Sprintf("transfer rejected: %s -> %s", req.GetSource(), req.GetTarget())}, nil
	}
	// operation committed, record durably...
	entry := &domain.WALEntry{
		CommandType: domain.TransferCommand,
		TransferInfo: &domain.WALTransfer{
			Source: req.GetSource(),
			Target: req.GetTarget(),
			Amount: req.GetAmount(),
			IdempotencyRecord: req.GetIdempotencyKey(),
		},
		InitialiseAccountInfo: nil,
	}
	if err := s.WAL.Append(entry); err != nil {
		return &pb.TransferResponse{
			Success: false,
			Message: "failed to persist: " + err.Error(),
		}, nil
	}
	
	return &pb.TransferResponse{
		Success: true,
		Message: fmt.Sprintf("transferred %d from %s to %s", req.GetAmount(), req.GetSource(), req.GetTarget()),
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
	
	n := len(s.Queues)

	// 1.5. Get actor index...
	idx := getActorIndex(initReq.Username, n)

	// 2. Slip queue into generic LedgerCommand.
	s.Queues[idx] <- domain.LedgerCommand{
		Type: domain.InitialiseAccountCommand,
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

	entry := &domain.WALEntry{
		CommandType: domain.InitialiseAccountCommand,
		TransferInfo: nil,
		InitialiseAccountInfo: &domain.WALInit{
			Username: req.GetUsername(),
			StartingBalance: req.GetBalance(),
		},
	}

	if err := s.WAL.Append(entry); err != nil {
		return &pb.InitialiseAccountResponse{
			Success: false,
			Message: "failed to persist: " + err.Error(),
		}, nil
	}

	return &pb.InitialiseAccountResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully initialised account %s with %d starting balance!", req.GetUsername(), req.GetBalance()),
	}, nil
}

// StartGRPCServer is a helper function to bind our server.
func StartGRPCServer(port string, queues  []chan domain.LedgerCommand, wal domain.WAL) (*g.Server, net.Listener, error) {
	
	// Open a standard TCP network port listener...
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, nil, err
	}

	// Create a new & empty gRPC server instance...
	gRPCServer := g.NewServer()

	// Instantiate our custom Server struct with the domain ledger...
	srv := &Server{
		Queues: queues,
		WAL: wal,
	}

	// Register our implementation with the gRPC server router...
	pb.RegisterLedgerServiceServer(gRPCServer, srv)

	return gRPCServer, listener, nil
}
