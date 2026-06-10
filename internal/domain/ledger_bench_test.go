package domain

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkConcurrentTransfers Low Contention simulates a healthy banking ecosystem
// where users are randomly transferring money across a wide array of accounts.
func BenchmarkConcurrentTransfers_LowContention(b *testing.B) {
	// 1. Initialize the master ledger
	ledger := InitialiseLedger()

	// 2. Provision a pool of 2,000 unique random accounts
	accountCount := 2000
	for i := range accountCount {
		username := fmt.Sprintf("user_%d", i)
		_ = ledger.InitialiseAccount(username, 100000) // Start everyone with plenty of cash
	}

	// An atomic counter to ensure every single concurrent operation gets a unique idempotency key
	var txCounter uint64

	b.ResetTimer()

	// 3. Spawns an aggressive parallel worker group maximizing all available CPU cores
	b.RunParallel(func(pb *testing.PB) {
		// Private, localized random source for this specific thread to avoid thread-locked random seeds
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&txCounter, 1))))

		for pb.Next() {
			// Pick two completely random accounts from our pool
			sourceIdx := r.Intn(accountCount)
			targetIdx := r.Intn(accountCount)

			if sourceIdx == targetIdx {
				continue
			}

			source := fmt.Sprintf("user_%d", sourceIdx)
			target := fmt.Sprintf("user_%d", targetIdx)

			// Generate a completely unique idempotency key for this atomic run
			idempotencyKey := fmt.Sprintf("key-%d", atomic.AddUint64(&txCounter, 1))

			// Fire the transfer straight into your alphabetical mutex locks!
			_, _ = ledger.Transfer(source, target, 1, idempotencyKey)
		}
	})
}

// BenchmarkConcurrentTransfers_HighContention simulates a crisis event (like pay-day)
// where thousands of concurrent threads are all desperately trying to move money out of
// or into the EXACT same popular account ("The Mint").
func BenchmarkConcurrentTransfers_HighContention(b *testing.B) {
	ledger := InitialiseLedger()

	accountCount := 1000
	for i := range accountCount {
		username := fmt.Sprintf("user_%d", i)
		_ = ledger.InitialiseAccount(username, 100000)
	}

	var txCounter uint64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&txCounter, 1))))

		for pb.Next() {
			// Everyone picks a random unique source account...
			sourceIdx := r.Intn(accountCount)
			source := fmt.Sprintf("user_%d", sourceIdx)

			// ...BUT EVERYONE TARGETS "THE MINT" AT THE EXACT SAME TIME!
			target := "The Mint"

			idempotencyKey := fmt.Sprintf("key-high-%d", atomic.AddUint64(&txCounter, 1))

			_, _ = ledger.Transfer(source, target, 1, idempotencyKey)
		}
	})
}

// BenchmarkActorTransfers_LowContention simulates our lock-free engine processing requests randomly...
func BenchmarkActorTransfers_LowContention(b *testing.B) {
	// Initialise the ledger and the command pipeline...
	ledger := InitialiseLedger()
	queue := make(chan LedgerCommand, 10000) // large buffer to prevent stalling...
	
	var wg sync.WaitGroup

	// Start the single background worker thread...
	ledger.StartActorLoop(queue, &wg)
	
	// Generate 2,000 unique accounts...
	accountCount := 2000
	for i := range accountCount {
		username := fmt.Sprintf("user_%d", i)
		_ = ledger.executePureInitialise(username, 100000)
	}

	var txCounter uint64
	b.ResetTimer()

	// Simulate thousands of concurrent gRPC worker threads hitting the channel...
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&txCounter, 1))))

		// Create a reusable response channel local to this worker thread...
		replyChan := make(chan TransferResponse, 1)

		for pb.Next() {
			sourceIdx := r.Intn(accountCount)
			targetIdx := r.Intn(accountCount)
			
			if sourceIdx == targetIdx {
				continue
			}

			req := &TransferRequest{
				Source:         fmt.Sprintf("user_%d", sourceIdx),
				Target:         fmt.Sprintf("user_%d", targetIdx),
				Amount:         1,
				IdempotencyKey: fmt.Sprintf("key-actor-%d", atomic.AddUint64(&txCounter, 1)),
				ReplyTo:        replyChan,
			}

			// Drop the request into the queue.
			queue <- LedgerCommand{
				Type:     "TRANSFER",
				Transfer: req,
			}

			// Block and freeze here until the single actor thread responds with the answer...
			res := <-replyChan
			_ = res.Success
		}
	})

	close(queue) // Clean up channel when done...
	
	wg.Wait()
}

// BenchmarkActorTransfers_HighContention simulates thousands of concurrent threads
func BenchmarkActorTransfers_HighContention(b *testing.B) {
	ledger := InitialiseLedger()
	queue := make(chan LedgerCommand, 10000)
	var wg sync.WaitGroup

	ledger.StartActorLoop(queue, &wg)

	accountCount := 1000
	for i := 0; i < accountCount; i++ {
		username := fmt.Sprintf("user_%d", i)
		_ = ledger.executePureInitialise(username, 100000)
	}

	var txCounter uint64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&txCounter, 1))))
		replyChan := make(chan TransferResponse, 1)

		for pb.Next() {
			sourceIdx := r.Intn(accountCount)
			
			req := &TransferRequest{
				Source:         fmt.Sprintf("user_%d", sourceIdx),
				Target:         "The Mint", // Every single thread targets the same account
				Amount:         1,
				IdempotencyKey: fmt.Sprintf("key-actor-high-%d", atomic.AddUint64(&txCounter, 1)),
				ReplyTo:        replyChan,
			}

			queue <- LedgerCommand{
				Type:     "TRANSFER",
				Transfer: req,
			}

			res := <-replyChan
			_ = res.Success
		}
	})

	close(queue)

	wg.Wait()
}
