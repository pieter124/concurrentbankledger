package main

import (
	"fmt"
	"time"
)

// Transaction struct represents the financial transaction that obeys our zero-sum rule.
type Transaction struct {
	ID        string    `json:"id"`        // Unique identifier of transaction.
	Source    string    `json:"source"`    // Username sending the money.
	Target    string    `json:"target"`    // Username receiving the money.
	Amount    int64     `json:"amount"`    // Always in pence (e.g. £10.50 = 1050p).
	Timestamp time.Time `json:"timestamp"` // Timestamp of the financial transaction.
}

// LocalAccountTransaction represents financial transactions local to an account, for faster derivation of the balance.
type LocalAccountTransaction struct {
	TransactionID string `json:"transactionid"` // Identifier of the transaction object it belongs to.
	Amount        int64  `json:"amount"`        // -ve for credit and +ve for debit.
}

// Account struct represents any financial entity allowed to perform financial transactions.
type Account struct {
	Username string                    `json:"username"` // Unique identifier of account.
	History  []LocalAccountTransaction `json:"history"`  // A list of transactions local to the account.
}

// Ledger struct represents the supported accounts and the global history of financial transactions.
type Ledger struct {
	Account       map[string]*Account `json:"accounts"`
	LedgerHistory []Transaction       `json:"ledgerhistory"`
}

func main() {
	fmt.Println("Thought Machine Ledger Project: Initialized.")
}
