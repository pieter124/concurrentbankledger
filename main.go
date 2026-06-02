package main

import (
	"fmt"
	"sync"

	"concurrent-bank-ledger/internal/domain"
)

func main() {
	mockLedger := domain.InitialiseLedger()
	mockLedger.InitialiseAccount("alice", 10_000)
	mockLedger.InitialiseAccount("bob", 10_000)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mockLedger.Transfer("alice", "bob", 100)
			mockLedger.Transfer("bob", "alice", 100)
		}()
	}

	wg.Wait()
	aliceBalance := mockLedger.Account["alice"].GetBalance()
	bobBalance := mockLedger.Account["bob"].GetBalance()
	fmt.Printf("Simulation complete...\nBob's Balance: %d\nAlice's Balance: %d\n", aliceBalance, bobBalance)
}
