package main

import (
	"log"

	"concurrent-bank-ledger/internal/domain"
	"concurrent-bank-ledger/internal/ports/grpc"
)

func main() {
	ledger := domain.InitialiseLedger()
	serverPort := ":8080"
	err := grpc.StartGRPCServer(serverPort, ledger)
	if err != nil {
		log.Fatalf("gRPC server crashed: %v", err)
	}
}
