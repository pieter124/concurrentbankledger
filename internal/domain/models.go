package domain

import (
	"sync"
	"time"
)

// An enum to help define the different types of states for our idempotency check.
const (
	StatusPending = iota
	StatusSuccess
	StatusFailedFunds
)

// TransferResponse struct is our response struct we use to ship through our channel.
type TransferResponse struct {
	Success bool
	Err     error
}

// TransferRequest struct holds the input fields required to execute a transfer, it needs to also have a return address channel to mail the TransferResponse back.
type TransferRequest struct {
	Source         string
	Target         string
	Amount         int64
	IdempotencyKey string
	ReplyTo        chan TransferResponse
}

// InitialiseAccountRequest holds the input fields required to build an account.
type InitialiseAccountRequest struct {
	Username        string
	StartingBalance int64
	ReplyTo         chan error
}

// LedgerCommand acts as a single object, so our channel can use it.
type LedgerCommand struct {
	Type        string
	Transfer    *TransferRequest
	InitAccount *InitialiseAccountRequest
}

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
	sync.Mutex                           // Anonymous approach (allows you to use explicit function calls).
	Username   string                    `json:"username"` // Unique identifier of account.
	History    []LocalAccountTransaction `json:"history"`  // A list of transactions local to the account.
}

// IdempotencyRecord struct helps to ensure transactions are not repeated and only done once.
type IdempotencyRecord struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
	Amount int64  `json:"amount"`
	Status int    `json:"status"`
}

// Ledger struct represents the supported accounts and the global history of financial transactions.
type Ledger struct {
	sync.Mutex                                          // Anonymous approach (allows you to use explicit function calls).
	Account               map[string]*Account           `json:"accounts"`
	AttemptedTransactions map[string]*IdempotencyRecord `json:"attemptedtransactions"`
	LedgerHistory         []Transaction                 `json:"ledgerhistory"`
}

/* BASIC UTILITIES */

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
