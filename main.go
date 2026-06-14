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

const (
	NoOfActors      = 8
	QueueBufferSize = 10000
	ServerPort      = "8080"
)

func main() {
	ledger := domain.InitialiseLedger()

	queues := make([]chan domain.LedgerCommand, NoOfActors)
	
	for i := range NoOfActors {
		queues[i] = make(chan domain.LedgerCommand, QueueBufferSize)
	}

	var wg sync.WaitGroup

	// Boot lock-free background engine loop...
	for i := range NoOfActors {
		ledger.StartActorLoop(queues[i], &wg)
	}

	// Pass the queue into the server runner instead of the ledger...
	// We can capture the running server object so we can stop it later...
	gRPCServer, listen, err := grpc.StartGRPCServer(ServerPort, queues)
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

	
	for i := range NoOfActors {
		close(queues[i])
	}
	wg.Wait()
}
