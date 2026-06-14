// Package domain contains the internal logic of our ledger and entities.
package domain

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// executePureTransfer runs sequentially inside the background worker thread.
// No locks are required here...
func (ledger *Ledger) executePureTransfer(source string, target string, amount int64, idempotencyKey string) (bool, error) {
	// Quick sanitation check of valid amount.
	if amount <= 0 {
		return false, nil
	}

	// Sanitize existence of accounts.
	if source == target {
		return false, nil
	}
	sourceAccount, exists := ledger.Account[source]
	if !exists {
		return false, nil
	}
	targetAccount, exists := ledger.Account[target]
	if !exists {
		return false, nil
	}

	// Idempotency check... now safe without ledger.Lock()
	record, exists := ledger.AttemptedTransactions[idempotencyKey]
	if exists {
		// Handle mismatch payload...
		if record.Source != source || record.Target != target || record.Amount != amount {
			return false, fmt.Errorf("payload mismatch for idempotency key %s", idempotencyKey)
		}

		// Handle identical match...
		switch record.Status {
		case StatusSuccess:
			return true, nil
		case StatusPending:
			return false, fmt.Errorf("transaction for key %s is already being processed", idempotencyKey)
		default:
			return false, nil
		}
	}

	// Construct pending idempotency record....
	ledger.AttemptedTransactions[idempotencyKey] = &IdempotencyRecord{
		ID:     idempotencyKey,
		Source: source,
		Target: target,
		Amount: amount,
		Status: StatusPending,
	}

	// Sanitize if the source account has enough money...
	if sourceBalance := sourceAccount.GetBalance(); sourceBalance < amount {
		if rec, ex := ledger.AttemptedTransactions[idempotencyKey]; ex {
			rec.Status = StatusFailedFunds
		}
		return false, fmt.Errorf("insufficient funds on key %s", idempotencyKey)
	}

	// Create transaction objects...
	id := uuid.NewString()
	globalTransaction := Transaction{
		ID:        id,
		Source:    source,
		Target:    target,
		Amount:    amount,
		Timestamp: time.Now(),
	}
	sourceTransaction := LocalAccountTransaction{
		TransactionID: id,
		Amount:        -amount,
	}
	targetTransaction := LocalAccountTransaction{
		TransactionID: id,
		Amount:        amount,
	}

	// Commit directly to state maps and histories!
	ledger.LedgerHistory = append(ledger.LedgerHistory, globalTransaction)
	if rec, ex := ledger.AttemptedTransactions[idempotencyKey]; ex {
		rec.Status = StatusSuccess
	}

	sourceAccount.History = append(sourceAccount.History, sourceTransaction)
	targetAccount.History = append(targetAccount.History, targetTransaction)

	return true, nil
}

// executePureInitialise creates an account sequentially inside the background worker thread... No locks required!
func (ledger *Ledger) executePureInitialise(username string, startingBalance int64) error {
	// Sanity check map existence safely without a lock.
	if _, exists := ledger.Account[username]; exists {
		return fmt.Errorf("account already exists with username %s", username)
	}

	if startingBalance == 0 {
		return fmt.Errorf("balance > 0 is required")
	}

	// Build entity struct...
	newAccount := Account{
		Username: username,
		History:  make([]LocalAccountTransaction, 0, 10),
	}

	// Insert directly into the map without ledger.Lock()
	ledger.Account[username] = &newAccount

	// Generate a unique idempotency key for this account's genesis transaction...
	genesisKey := "genesis-funding-" + username

	// Call lock-free transfer directly...
	_, err := ledger.executePureTransfer("The Mint", username, startingBalance, genesisKey)
	if err != nil {
		return fmt.Errorf("could not initialise account")
	}
	return nil
}

// StartActorLoop boots up the single-threaded engine processor.
// It continuously reads commands from the queue and executes them lock-free.
func (ledger *Ledger) StartActorLoop(queue chan LedgerCommand, wg *sync.WaitGroup) {
	wg.Go(func() {
		for cmd := range queue {
			switch cmd.Type {

			case TransferCommand:
				req := cmd.Transfer
				// Run the pure sequential math safely on 1 core
				success, err := ledger.executePureTransfer(req.Source, req.Target, req.Amount, req.IdempotencyKey)
				// Mail the bundled response struct straight back to the waiting caller
				req.ReplyTo <- TransferResponse{Success: success, Err: err}

			case InitialiseAccountCommand:
				req := cmd.InitAccount
				// Run the pure sequential initialisation safely on 1 core
				err := ledger.executePureInitialise(req.Username, req.StartingBalance)
				// Mail the single error object back to the waiting caller
				req.ReplyTo <- err
			}
		}
	})
}
