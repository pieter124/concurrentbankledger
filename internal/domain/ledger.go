// Package domain contains the internal logic of our ledger and entities.
package domain

import (
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
func (ledger *Ledger) Transfer(source string, target string, amount int64) bool {
	// Quick sanitation check of valid amount.
	if amount <= 0 {
		return false
	}

	// Sanitize existence of accounts.
	if source == target {
		return false
	}
	sourceAccount, exists := ledger.Account[source]
	if !exists {
		return false
	}
	targetAccount, exists := ledger.Account[target]
	if !exists {
		return false
	}

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
		return false
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
	ledger.Unlock()

	sourceAccount.History = append(sourceAccount.History, sourceTransaction)
	targetAccount.History = append(targetAccount.History, targetTransaction)

	return true
}

// InitialiseAccount - Initialising new accounts by using "The Mint" to model the starting balance of the new account as one big transaction.
func (ledger *Ledger) InitialiseAccount(username string, startingBalance int64) {
	newAccount := Account{
		Username: username,
		History:  make([]LocalAccountTransaction, 0, 10),
	}
	ledger.Lock()
	ledger.Account[username] = &newAccount
	ledger.Unlock()
	ledger.Transfer("The Mint", username, startingBalance)
}

// InitialiseLedger - Initializing a ledger, creating a "system mint", which essentially has infinite monies.
func InitialiseLedger() (ledger Ledger) {
	ledger = Ledger{
		Account:       make(map[string]*Account),
		LedgerHistory: make([]Transaction, 0, 100),
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
