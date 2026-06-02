package main

import (
	"fmt"
	"sync"
	"time"
	"github.com/google/uuid"
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
	sync.Mutex                           // Anonymous approach (allows you to use explicit function calls).
	Username   string                    `json:"username"` // Unique identifier of account.
	History    []LocalAccountTransaction `json:"history"`  // A list of transactions local to the account.
}

// Ledger struct represents the supported accounts and the global history of financial transactions.
type Ledger struct {
	sync.Mutex // Anonymous approach (allows you to use explicit function calls).
	Account       map[string]*Account `json:"accounts"`
	LedgerHistory []Transaction       `json:"ledgerhistory"`
}

// Simple function to sum up the transactions of an account, to get the current balance.
func (account *Account) GetBalance() int64 {
	var balance int64
	for i := range account.History {
		transaction := account.History[i]
		amount := transaction.Amount
		balance += amount
	}
	return balance
}

// Thread-safe transferring of finances from one account to another.
func (ledger *Ledger) Transfer(source string, target string, amount int64) bool {
	// Sanitize existence of accounts.
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
		ID: id, 
		Source: source, 
		Target: target, 
		Amount: amount, 
		Timestamp: time.Now(), 
	}
	sourceTransaction := LocalAccountTransaction{
		TransactionID: id, 
		Amount: -amount, 
	}
	targetTransaction := LocalAccountTransaction{
		TransactionID: id,
		Amount: amount,
	}

	// Lock ledger...
	ledger.Lock()
	ledger.LedgerHistory = append(ledger.LedgerHistory, globalTransaction)
	ledger.Unlock()

	sourceAccount.History = append(sourceAccount.History, sourceTransaction)
	targetAccount.History = append(targetAccount.History, targetTransaction)

	return true
}

// Initialising new accounts by using "The Mint" to model the starting balance of the new account as one big transaction.
func (ledger *Ledger) InitialiseAccount(username string, startingBalance int64) {
	newAccount := Account{
		Username: username,
		History: make([]LocalAccountTransaction, 0, 10),
	}
	ledger.Lock()
	ledger.Account[username] = &newAccount
	ledger.Unlock()
	ledger.Transfer("The Mint", username, startingBalance)
}

// Initializing a ledger, creating a "system mint", which essentially has infinite monies.
func InitializeLedger() (ledger Ledger) {
	ledger = Ledger{
		Account: make(map[string]*Account),
		LedgerHistory: make([]Transaction, 0, 100),
	}
	ledger.Account["The Mint"] = &Account{
		Username: "The Mint",
		History: make([]LocalAccountTransaction, 0, 10),
	}
	ledger.Account["The Mint"].History = append(ledger.Account["The Mint"].History, LocalAccountTransaction{
		TransactionID: "THEMINT",
		Amount: 999_999_999_999,
		})
	return
}

func main() {
	mockLedger := InitializeLedger()
	mockLedger.InitialiseAccount("alice", 10_000)
	mockLedger.InitialiseAccount("bob", 10_000)
	
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			mockLedger.Transfer("alice", "bob", 100)
			mockLedger.Transfer("bob", "alice", 100)
		})
	}

	wg.Wait()
	aliceBalance := mockLedger.Account["alice"].GetBalance()
	bobBalance := mockLedger.Account["bob"].GetBalance()
	fmt.Printf("Simulation complete...\nBob's Balance: %d\nAlice's Balance: %d\n", aliceBalance, bobBalance)
 }
