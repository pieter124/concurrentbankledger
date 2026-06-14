package domain

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"hash/fnv"
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
				Type:     TransferCommand,
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
				Type:     TransferCommand,
				Transfer: req,
			}

			res := <-replyChan
			_ = res.Success
		}
	})

	close(queue)

	wg.Wait()
}


func getActorIndex(key string, NoOfActors int) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))

	return int(hasher.Sum32()) % NoOfActors
} 

func BenchmarkShardedTransfers_LowContention(b *testing.B) {
	const shards, accounts = 8, 2000

	// One ledger per queue — each actor owns its own state.
	ledgers := make([]*Ledger, shards)
	queues := make([]chan LedgerCommand, shards)
	var wg sync.WaitGroup
	for i := range shards {
		ledgers[i] = InitialiseLedger()
		queues[i] = make(chan LedgerCommand, 10000)
		ledgers[i].StartActorLoop(queues[i], &wg)
	}

	shardOf := func(name string) int { return getActorIndex(name, shards) }

	// Seed: create each account on its own shard and fund it locally.
	for i := range accounts {
		u := fmt.Sprintf("user_%d", i)
		_ = ledgers[shardOf(u)].executePureInitialise(u, 100000)
	}

	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&ctr, 1))))
		for pb.Next() {
			s, t := r.Intn(accounts), r.Intn(accounts)
			if s == t {
				continue
			}
			source, target := fmt.Sprintf("user_%d", s), fmt.Sprintf("user_%d", t)
			key := fmt.Sprintf("key-sharded-%d", atomic.AddUint64(&ctr, 1))
			si, ti := shardOf(source), shardOf(target)

			if si == ti {
				// Same shard: one message.
				reply := make(chan TransferResponse, 1)
				queues[si] <- LedgerCommand{Type: TransferCommand,
					Transfer: &TransferRequest{source, target, 1, key, reply}}
				<-reply
				continue
			}

			// Cross shard: reserve -> credit -> finalize (refund on failure).
			rRep := make(chan ReserveResponse, 1)
			queues[si] <- LedgerCommand{Type: ReserveCommand,
				Reserve: &ReserveRequest{source, target, 1, key, rRep}}
			res := <-rRep
			if !res.Proceed {
				continue
			}
			cRep := make(chan TransferResponse, 1)
			queues[ti] <- LedgerCommand{Type: CreditCommand,
				Credit: &CreditRequest{target, 1, res.TxnID, cRep}}
			if c := <-cRep; !c.Success {
				fRep := make(chan TransferResponse, 1)
				queues[si] <- LedgerCommand{Type: RefundCommand, Refund: &KeyedRequest{key, fRep}}
				<-fRep
				continue
			}
			fRep := make(chan TransferResponse, 1)
			queues[si] <- LedgerCommand{Type: FinalizeCommand, Finalize: &KeyedRequest{key, fRep}}
			<-fRep
		}
	})

	b.StopTimer()
	for _, q := range queues {
		close(q)
	}
	wg.Wait()
}

func BenchmarkShardedTransfers_HighContention(b *testing.B) {
	const shards, accounts = 8, 1000

	ledgers := make([]*Ledger, shards)
	queues := make([]chan LedgerCommand, shards)
	var wg sync.WaitGroup
	for i := range shards{
		ledgers[i] = InitialiseLedger()
		queues[i] = make(chan LedgerCommand, 10000)
		ledgers[i].StartActorLoop(queues[i], &wg)
	}

	shardOf := func(name string) int { return getActorIndex(name, shards) }

	for i := range accounts {
		u := fmt.Sprintf("user_%d", i)
		_ = ledgers[shardOf(u)].executePureInitialise(u, 100000)
	}

	var ctr uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(int64(atomic.AddUint64(&ctr, 1))))
		for pb.Next() {
			s := r.Intn(accounts)
			source := fmt.Sprintf("user_%d", s)
			target := "The Mint" // everyone funnels into the one hot account
			key := fmt.Sprintf("key-sharded-high-%d", atomic.AddUint64(&ctr, 1))
			si, ti := shardOf(source), shardOf(target)

			if si == ti {
				reply := make(chan TransferResponse, 1)
				queues[si] <- LedgerCommand{Type: TransferCommand,
					Transfer: &TransferRequest{source, target, 1, key, reply}}
				<-reply
				continue
			}

			rRep := make(chan ReserveResponse, 1)
			queues[si] <- LedgerCommand{Type: ReserveCommand,
				Reserve: &ReserveRequest{source, target, 1, key, rRep}}
			res := <-rRep
			if !res.Proceed {
				continue
			}
			cRep := make(chan TransferResponse, 1)
			queues[ti] <- LedgerCommand{Type: CreditCommand,
				Credit: &CreditRequest{target, 1, res.TxnID, cRep}}
			if c := <-cRep; !c.Success {
				fRep := make(chan TransferResponse, 1)
				queues[si] <- LedgerCommand{Type: RefundCommand, Refund: &KeyedRequest{key, fRep}}
				<-fRep
				continue
			}
			fRep := make(chan TransferResponse, 1)
			queues[si] <- LedgerCommand{Type: FinalizeCommand, Finalize: &KeyedRequest{key, fRep}}
			<-fRep
		}
	})

	b.StopTimer()
	for _, q := range queues {
		close(q)
	}
	wg.Wait()
}
