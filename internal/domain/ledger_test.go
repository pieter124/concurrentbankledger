package domain

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentLedgerInvariance(t *testing.T) {
	// Initialise ledger and create accounts...
	ledger := InitialiseLedger()
	_ = ledger.InitialiseAccount("alice", 10_000)
	_ = ledger.InitialiseAccount("bob", 10_000)

	// Calculate initial total monies...
	initialMonies := ledger.Account["The Mint"].GetBalance() +
		ledger.Account["alice"].GetBalance() +
		ledger.Account["bob"].GetBalance()

	// Execute concurrent financial transactions...
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(2)

		// Worker 1 moves money from alice to bob.
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("tx_aliceToBob%d", id)
			_, _ = ledger.Transfer("alice", "bob", 50, key)
		}(i)

		// Worker 2 moves money from bob to alice.
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("tx_bobToAlice%d", id)
			_, _ = ledger.Transfer("bob", "alice", 50, key)
		}(i)
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
	_ = ledger.InitialiseAccount("alice", 1000)
	_ = ledger.InitialiseAccount("bob", 500)

	// Alice tries to send more than she has.
	success, _ := ledger.Transfer("alice", "bob", 2000, "tx_aliceToBob")

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
	_ = ledger.InitialiseAccount("alice", 1000)

	// Attempt transfer to non-existent account.
	success, _ := ledger.Transfer("alice", "charlie", 500, "tx_aliceToCharlie")

	if success {
		t.Fatalf("Expected transfer to non-existent account to fail, but did not.")
	}
	if balance := ledger.Account["alice"].GetBalance(); balance != 1000 {
		t.Fatalf("Alice's balance should remain unchanged.\nGot: %d\nExpected: 1000", balance)
	}
}

func TestTransferInvalidAmount(t *testing.T) {
	ledger := InitialiseLedger()
	_ = ledger.InitialiseAccount("alice", 1000)
	_ = ledger.InitialiseAccount("bob", 1000)

	// Attempt negative transfer.
	if success, _ := ledger.Transfer("alice", "bob", -500, "tx_aliceToBob1"); success {
		t.Fatalf("Ledger allowed transfer with a negative amount. Big security flaw...")
	}
	if success, _ := ledger.Transfer("alice", "bob", 0, "tx_aliceToBob2"); success {
		t.Fatalf("Ledger allowed transfer of 0p; should be rejected as an invalid operation...")
	}
	if success, _ := ledger.Transfer("alice", "alice", 1, "tx_aliceToAlice"); success {
		t.Fatalf("Ledger allowed transfer of money from source to target; should be rejected as an invalid operation...")
	}
}

func TestTransferIdempotencyAndHijackProtection(t *testing.T) {
	ledger := InitialiseLedger()
	_ = ledger.InitialiseAccount("alice", 1000)
	_ = ledger.InitialiseAccount("bob", 500)

	sharedKey := "tx_idempotency_test_key"

	// Alice sends 200p to Bob. Should succeed...
	success1, err1 := ledger.Transfer("alice", "bob", 200, sharedKey)
	if !success1 || err1 != nil {
		t.Fatalf("First legitimate transfer failed: %v", err1)
	}

	// Verify the money actually moved once...
	if bal := ledger.Account["alice"].GetBalance(); bal != 800 {
		t.Errorf("Expected Alice to have 800p after first transfer, got: %d", bal)
	}

	// The "network retry"... Send the exact same payload and key.
	// It should return success and not deduct money from Alice's account...
	success2, err2 := ledger.Transfer("alice", "bob", 200, sharedKey)
	if !success2 || err2 != nil {
		t.Fatalf("Idempotent retry failed to process silently: %v", err2)
	}

	// Verify the balance did NOT drot...
	if bal := ledger.Account["alice"].GetBalance(); bal != 800 {
		t.Fatalf("Security Flaw: Idempotent retry mutated the ledger balance again! Alice balance: %d", bal)
	}
	if len(ledger.LedgerHistory) != 3 { // 2 genesis seedings + 1 real transfer
		t.Fatalf("Security flaw... Duplicate entry appended to global history. Total logs: %d", len(ledger.LedgerHistory))
	}

	// Now try with keeping the key, but alter the amount to 5000p.
	// Your guard must block this and return a validation error.
	success3, err3 := ledger.Transfer("alice", "bob", 5000, sharedKey)
	if success3 {
		t.Fatalf("Security flaw... Ledger allowed a modified payload using a recycled idempotency key.")
	}
	if err3 == nil {
		t.Fatalf("Expected a payload mismatch error, but received a nil error interface")
	}
}
