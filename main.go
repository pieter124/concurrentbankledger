package main

import (
	"log"
	
	"concurrent-bank-ledger/internal/ports/grpc"
	"concurrent-bank-ledger/internal/domain"
)

func main() {
	ledger := domain.InitialiseLedger()
	ledger.InitialiseAccount("wendy", 5000)
	ledger.InitialiseAccount("pieter", 5)
	
	serverPort := ":8080"
	err := grpc.StartGRPCServer(serverPort, ledger)
	if err != nil {
		log.Fatalf("gRPC server crashed: %v", err)
	}
}
