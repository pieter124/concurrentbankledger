package domain

// WAL interface defines the contract any WAL implementation needs to adhere to...
type WAL interface {
	Append(entry *WALEntry) error
	Replay() []WALEntry
}

// An enum to help define the status of the request in the WAL entry..
const (
	Pending = iota
	Committed
	Failed
	Aborted
)

// WALEntry is the struct that records the client's request.
type WALEntry struct {
	RequestType           int // TransferCommand or InitialiseAccountCommand stricty..
	RequestStatus         int // e.g. Pending, Committed
	TransferInfo          *WALTransfer
	InitialiseAccountInfo *WALInit
}

// WALTransfer is a struct that holds all the required fields for the WAL to hold a transfer request entry...
type WALTransfer struct {
	Source            string
	Target            string
	Amount            int64
	IdempotencyRecord int64
}

// WALInit is a struct that holds all the required fields for the WAL to hold an initialise account request entry...
type WALInit struct {
	Username        string
	StartingBalance int64
}

// FileWAL is a file-based implementation of the WAL, append-only to the global.wal file.
type FileWAL struct {
	Entries []WALEntry
}

