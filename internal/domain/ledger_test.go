package domain

import (
	"sync"
	"testing"
)

func TestConcurrentLedgerInvariance(t *testing.T) {
	// Initialise ledger and create accounts...
	ledger := InitialiseLedger()
	ledger.InitialiseAccount("alice", 10_000)
	ledger.InitialiseAccount("bob", 10_000)

	// Calculate initial total monies...
	initialMonies := ledger.Account["The Mint"].GetBalance() + 
		ledger.Account["alice"].GetBalance() +
		ledger.Account["bob"].GetBalance()

	// Execute concurrent financial transactions...
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(2)
		
		// Worker 1 moves money from alice to bob.
		go func() {
			defer wg.Done()
			ledger.Transfer("alice", "bob", 50)
		}()

		// Worker 2 moves money frmo bob to alice.
		go func() {
			defer wg.Done()
			ledger.Transfer("alice", "bob", 50)
		}()
	}
	wg.Wait()

	// Calculate final total monies...
	finalMonies := ledger.Account["The Mint"].GetBalance() +
		ledger.Account["alice"].GetBalance() +
		ledger.Account["bob"].GetBalance()

	// If money was created or destroyed, fail instantly!
	if initialMonies != finalMonies {
		t.Fatalf("Zero-sum violation.\nInitial sum: %d\nFinal sum: %d", initialMonies, finalMonies)
	}
}

func TestTransferInsufficientFunds(t *testing.T) {
	// Initialise ledger and accounts...
	ledger := InitialiseLedger()
	ledger.InitialiseAccount("alice", 1000)
	ledger.InitialiseAccount("bob", 500)
	
	// Alice tries to send more than she has.
	success := ledger.Transfer("alice", "bob", 2000)
	
	if success {
		t.Fatalf("Expected transfer to fail due to insufficient funds but did not.")
	}
	if balance := ledger.Account["alice"].GetBalance(); balance != 1000 {
		t.Fatalf("Alice's balance was modified on failure.\nGot: %d\nExpected: 1000", balance)
	}
	if balance := ledger.Account["bob"].GetBalance(); balance != 500 {
		t.Fatalf("Bob's balance was modified on failure.\nGot: %d\nExpected: 500", balance)
	}
}

func TestTransferToNonExistentAccount(t *testing.T) {
	// Initialise ledger and account.
	ledger := InitialiseLedger()
	ledger.InitialiseAccount("alice", 1000)
	
	// Attempt transfer to non-existent account.
	success := ledger.Transfer("alice", "charlie", 500)

	if success {
		t.Fatalf("Expected transfer to non-existent account to fail, but did not.")
	}
	if balance := ledger.Account["alice"].GetBalance(); balance != 1000 {
		t.Fatalf("Alice's balance should remain unchanged.\nGot: %d\nExpected: 1000", balance)
	}
}

func TestTransferInvalidAmount(t *testing.T) {
	ledger := InitialiseLedger()
	ledger.InitialiseAccount("alice", 1000)
	ledger.InitialiseAccount("bob", 1000)

	// Attempt negative transfer.
	if success := ledger.Transfer("alice", "bob", -500); success {
		t.Fatalf("Ledger allowed transfer with a negative amount. Big security flaw...")
	}
	if success := ledger.Transfer("alice", "bob", 0); success {
		t.Fatalf("Ledger allowed transfer of 0p; should be rejected as an invalid operation...")
	}
	if success := ledger.Transfer("alice", "alice", 1); success {
		t.Fatalf("Ledger allowed transfer of money from source to target; should be rejected as an invalid operation...")
	}
}
