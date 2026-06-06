// Package domain contains the internal logic of our ledger and entities.
package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GetBalance - Simple function to sum up the transactions of an account, to get the current balance.
func (account *Account) GetBalance() int64 {
	var balance int64
	for i := range account.History {
		transaction := account.History[i]
		amount := transaction.Amount
		balance += amount
	}
	return balance
}

// Transfer - Thread-safe transferring of finances from one account to another.
func (ledger *Ledger) Transfer(source string, target string, amount int64, idempotencyKey string) (bool, error) {
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

	// Idempotency check...
	ledger.Lock()
	record, exists := ledger.AttemptedTransactions[idempotencyKey]
	if exists {
		// Handle mismatch payload...
		if record.Source != source || record.Target != target || record.Amount != amount {
			ledger.Unlock()
			return false, fmt.Errorf("payload mismatch for idempotency key %s", idempotencyKey)
		}

		// Handle an identical match...
		switch record.Status {
		case StatusSuccess:
			ledger.Unlock()
			return true, nil
		case StatusPending:
			ledger.Unlock()
			return false, fmt.Errorf("transaction for key %s is already being processed", idempotencyKey)
		default:
			ledger.Unlock()
			return false, nil
		}

	}

	// Construct idempotency record...
	ledger.AttemptedTransactions[idempotencyKey] = &IdempotencyRecord{
		ID:     idempotencyKey,
		Source: source,
		Target: target,
		Amount: amount,
		Status: StatusPending,
	}
	ledger.Unlock()

	// Lock both accounts in an enforced order.
	// Then create and record the transactions.
	var first, second *Account
	if source < target {
		first = sourceAccount
		second = targetAccount
	} else {
		first = targetAccount
		second = sourceAccount
	}
	first.Lock()
	defer first.Unlock()
	second.Lock()
	defer second.Unlock()

	// Sanitize if the source account has enough money...
	if sourceBalance := sourceAccount.GetBalance(); sourceBalance < amount {
		ledger.Lock()
		if record, exists := ledger.AttemptedTransactions[idempotencyKey]; exists {
			record.Status = StatusFailedFunds
		}
		ledger.Unlock()
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

	// Lock ledger...
	ledger.Lock()
	ledger.LedgerHistory = append(ledger.LedgerHistory, globalTransaction)
	if record, exists := ledger.AttemptedTransactions[idempotencyKey]; exists {
		record.Status = StatusSuccess
	}
	ledger.Unlock()

	sourceAccount.History = append(sourceAccount.History, sourceTransaction)
	targetAccount.History = append(targetAccount.History, targetTransaction)

	return true, nil
}

// InitialiseAccount - Initialising new accounts by using "The Mint" to model the starting balance of the new account as one big transaction.
func (ledger *Ledger) InitialiseAccount(username string, startingBalance int64) error {
	// Sanity check
	if _, exists := ledger.Account[username]; exists {
		return fmt.Errorf("account already exists with username %s", username)
	}

	if startingBalance == 0 {
		return fmt.Errorf("balance > 0 is required")
	}

	newAccount := Account{
		Username: username,
		History:  make([]LocalAccountTransaction, 0, 10),
	}
	ledger.Lock()
	ledger.Account[username] = &newAccount
	ledger.Unlock()

	// Generate a unique idempotency key for this account's genesis transaction.
	genesisKey := "genesis-funding-" + username
	_, err := ledger.Transfer("The Mint", username, startingBalance, genesisKey)
	if err != nil {
		return fmt.Errorf("could not initialise account")
	}
	return nil
}

// InitialiseLedger - Initializing a ledger, creating a "system mint", which essentially has infinite monies.
func InitialiseLedger() (ledger *Ledger) {
	ledger = &Ledger{
		Account:               make(map[string]*Account),
		LedgerHistory:         make([]Transaction, 0, 100),
		AttemptedTransactions: make(map[string]*IdempotencyRecord),
	}
	ledger.Account["The Mint"] = &Account{
		Username: "The Mint",
		History:  make([]LocalAccountTransaction, 0, 10),
	}
	ledger.Account["The Mint"].History = append(ledger.Account["The Mint"].History, LocalAccountTransaction{
		TransactionID: "THEMINT",
		Amount:        999_999_999_999,
	})
	return
}
