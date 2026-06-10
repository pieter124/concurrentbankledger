package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"concurrent-bank-ledger/internal/domain"
	"concurrent-bank-ledger/internal/ports/grpc"

	g "google.golang.org/grpc"
)

func main() {
	ledger := domain.InitialiseLedger()
	queue := make(chan domain.LedgerCommand, 10000)

	var wg sync.WaitGroup

	// Boot lock-free background engine loop...
	ledger.StartActorLoop(queue, &wg)

	serverPort := ":8080"

	// Pass the queue into the server runner instead of the ledger...
	// We can capture the running server object so we can stop it later...
	gRPCServer, listen, err := grpc.StartGRPCServer(serverPort, queue)
	if err != nil {
		log.Fatalf("gRPC server crashed: %v", err)
	}

	// Start serving network traffic in an independent background thread...
	go func() {
		if err := gRPCServer.Serve(listen); err != nil && err != g.ErrServerStopped {
			log.Printf("gRPC server runtime failure: %v", err)
		}
	}()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownSignal
	gRPCServer.GracefulStop()

	close(queue)
	wg.Wait()
}
