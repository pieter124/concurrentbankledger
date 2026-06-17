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

// executeReserve runs on the SOURCE shard. It holds the funds and records a pending txn.
// It deliberately does NOT look at the target.
func (ledger *Ledger) executeReserve(source, target string, amount int64, key string) ReserveResponse {
	if amount <= 0 {
		return ReserveResponse{}
	}

	sourceAccount, exists := ledger.Account[source]
	if !exists {
		return ReserveResponse{}
	}

	// Idempotency check..
	if rec, ok := ledger.AttemptedTransactions[key]; ok {
		if rec.Source != source || rec.Target != target || rec.Amount != amount {
			return ReserveResponse{Err: fmt.Errorf("payload mismatch for key %s", key)}
		}
		switch rec.Status {
		case StatusSuccess:
			return ReserveResponse{TxnID: rec.ID, AlreadyDone: true} // already fully done — do NOT credit again
		case StatusPending:
			return ReserveResponse{Err: fmt.Errorf("key %s is already being processed", key)}
		default:
			return ReserveResponse{Err: fmt.Errorf("key %s previously failed", key)}
		}
	}

	// Funds check BEFORE any mutation, so a rejected reserve leaves source untouched.
	if sourceAccount.GetBalance() < amount {
		ledger.AttemptedTransactions[key] = &IdempotencyRecord{
			ID: key, Source: source, Target: target, Amount: amount, Status: StatusFailedFunds,
		}
		return ReserveResponse{Err: fmt.Errorf("insufficient funds on key %s", key)}
	}

	// Hold the money: mint an id, record pending, subtract from source, log the global txn.
	id := uuid.NewString()
	ledger.AttemptedTransactions[key] = &IdempotencyRecord{
		ID: id, Source: source, Target: target, Amount: amount, Status: StatusPending,
	}
	ledger.LedgerHistory = append(ledger.LedgerHistory, Transaction{
		ID: id, Source: source, Target: target, Amount: amount, Timestamp: time.Now(),
	})
	sourceAccount.History = append(sourceAccount.History, LocalAccountTransaction{
		TransactionID: id, Amount: -amount,
	})

	return ReserveResponse{TxnID: id, Proceed: true}
}

// executeCredit runs on the TARGET shard.
func (ledger *Ledger) executeCredit(target string, amount int64, txnID string) (bool, error) {
	targetAccount, exists := ledger.Account[target]
	if !exists {
		return false, fmt.Errorf("target account %s does not exist", target)
	}
	targetAccount.History = append(targetAccount.History, LocalAccountTransaction{
		TransactionID: txnID,
		Amount:        amount, // +ve: money arriving
	})
	return true, nil
}

// executeFinalize runs on the SOURCE shard. After credit confirms, flip pending -> success.
func (ledger *Ledger) executeFinalize(key string) (bool, error) {
	rec, ok := ledger.AttemptedTransactions[key]
	if !ok {
		return false, fmt.Errorf("no reservation to finalize for key %s", key)
	}
	rec.Status = StatusSuccess
	return true, nil
}

// executeRefund runs on the SOURCE shard. If credit failed, give the held money back
func (ledger *Ledger) executeRefund(key string) (bool, error) {
	rec, ok := ledger.AttemptedTransactions[key]
	if !ok || rec.Status != StatusPending {
		return false, nil
	}
	sourceAccount, exists := ledger.Account[rec.Source]
	if !exists {
		return false, fmt.Errorf("refund: source %s missing", rec.Source)
	}
	sourceAccount.History = append(sourceAccount.History, LocalAccountTransaction{
		TransactionID: rec.ID,
		Amount:        rec.Amount, // +ve: money returning to source
	})
	rec.Status = StatusFailedFunds
	return true, nil
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

			case ReserveCommand:
				r := cmd.Reserve
				r.ReplyTo <- ledger.executeReserve(r.Source, r.Target, r.Amount, r.IdempotencyKey)

			case CreditCommand:
				r := cmd.Credit
				ok, err := ledger.executeCredit(r.Target, r.Amount, r.TxnID)
				r.ReplyTo <- TransferResponse{Success: ok, Err: err}

			case FinalizeCommand:
				ok, err := ledger.executeFinalize(cmd.Finalize.IdempotencyKey)
				cmd.Finalize.ReplyTo <- TransferResponse{Success: ok, Err: err}

			case RefundCommand:
				ok, err := ledger.executeRefund(cmd.Refund.IdempotencyKey)
				cmd.Refund.ReplyTo <- TransferResponse{Success: ok, Err: err}
			}
		}
	})
}
