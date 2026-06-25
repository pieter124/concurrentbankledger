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
	Queues []chan domain.LedgerCommand
	WAL    domain.WAL
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

func (s *Server) RouteTransfer(source string, target string, amount int64, key string) (bool, error) {
	n := len(s.Queues)
	srcIdx := domain.GetIndex(source, n)
	tgtIdx := domain.GetIndex(target, n)

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
	ok, err := s.RouteTransfer(req.GetSource(), req.GetTarget(), req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return &pb.TransferResponse{Success: false, Message: err.Error()}, nil
	}
	if !ok {
		return &pb.TransferResponse{
			Success: false,
			Message: fmt.Sprintf("transfer rejected: %s -> %s", req.GetSource(), req.GetTarget()),
		}, nil
	}
	// operation committed, record durably...
	entry := &domain.WALEntry{
		CommandType: domain.TransferCommand,
		TransferInfo: &domain.WALTransfer{
			Source:            req.GetSource(),
			Target:            req.GetTarget(),
			Amount:            req.GetAmount(),
			IdempotencyKey: req.GetIdempotencyKey(),
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

func (s *Server) RouteInit(username string, startingBalance int64) error {
	
	// 1. Allocate response channel...
	replyChan := make(chan error, 1)
	initReq := &domain.InitialiseAccountRequest{
		Username:        username,
		StartingBalance: startingBalance,
		ReplyTo:         replyChan,
	}

	// 2. Get actor index...
	idx := domain.GetIndex(username, len(s.Queues))
	
	// 3. Slip queue into generic LedgerCommand...
	s.Queues[idx] <- domain.LedgerCommand{
		Type: domain.InitialiseAccountCommand,
		InitAccount: initReq,
	}

	err := <-replyChan
	if err != nil {
		return err
	}

	// Fund from mint, routes cross shard automatically...
	ok, err := s.RouteTransfer("The Mint", username, startingBalance, "genesis-" + username)
	if err != nil {
		return fmt.Errorf("genesis funding failed for %s: %w", username, err)
	}
	if !ok {
		return fmt.Errorf("genesis funding rejected for %s", username)
	}
	return nil
}

func (s *Server) InitialiseAccount(ctx context.Context, req *pb.InitialiseAccountRequest) (*pb.InitialiseAccountResponse, error) {
	// 1. Allocate response channel...
	err := s.RouteInit(req.GetUsername(), req.GetBalance())
	if err != nil {
		return &pb.InitialiseAccountResponse{Success: false, Message: err.Error()}, nil
	}
	
	entry := &domain.WALEntry{
		CommandType:  domain.InitialiseAccountCommand,
		TransferInfo: nil,
		InitialiseAccountInfo: &domain.WALInit{
			Username:        req.GetUsername(),
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

// Recover replays persisted WAL entries back into the ledger on startup...
func (s *Server) Recover() error {
	entries, err := s.WAL.Replay()
	if err != nil {
		return fmt.Errorf("WAL replay failed: %w", err)
	}

	for _, entry := range entries {
		switch entry.CommandType {
		case domain.TransferCommand:
			t := entry.TransferInfo
			ok, err := s.RouteTransfer(t.Source, t.Target, t.Amount, t.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("replay transfer failed: %w", err)
			}
			if !ok {
				return fmt.Errorf("replay transfer was rejected (source=%s target=%s)", t.Source, t.Target)
			}

		case domain.InitialiseAccountCommand:
			a := entry.InitialiseAccountInfo
			if err := s.RouteInit(a.Username, a.StartingBalance); err != nil {
				return fmt.Errorf("replay init failed: %w", err)
			}
		}
	}
	return nil
}

// StartGRPCServer is a helper function to bind our server.
func StartGRPCServer(port string, queues []chan domain.LedgerCommand, wal domain.WAL) (*g.Server, net.Listener, error) {
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
		WAL:    wal,
	}

	if err := srv.Recover(); err != nil {
		return nil, nil, err
	}
	// Register our implementation with the gRPC server router...
	pb.RegisterLedgerServiceServer(gRPCServer, srv)

	return gRPCServer, listener, nil
}
