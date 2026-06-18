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
	ServerPort      = ":8080"
)

func main() {
	queues := make([]chan domain.LedgerCommand, NoOfActors)
	ledgers := make([]*domain.Ledger, NoOfActors) // one ledger per shard

	for i := range NoOfActors {
		queues[i] = make(chan domain.LedgerCommand, QueueBufferSize)
		ledgers[i] = domain.InitialiseLedger()
	}

	var wg sync.WaitGroup
	// Each actor loop drains ITS OWN queue into ITS OWN ledger — no shared state.
	for i := range NoOfActors {
		ledgers[i].StartActorLoop(queues[i], &wg)
	}

	wal, err := domain.NewFileWAL("ledger.wal")
	if err != nil {
		log.Fatalf("could not open WAL: %v", err)
	}

	gRPCServer, listen, err := grpc.StartGRPCServer(ServerPort, queues, wal)
	if err != nil {
		log.Fatalf("gRPC server crashed: %v", err)
	}

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

