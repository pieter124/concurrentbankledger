package domain

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// WAL interface defines the contract any WAL implementation needs to adhere to...
type WAL interface {
	Append(entry *WALEntry) error
	Replay() ([]WALEntry, error)
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
	CommandType           int // TransferCommand or InitialiseAccountCommand stricty..
	TransferInfo          *WALTransfer
	InitialiseAccountInfo *WALInit
}

// WALTransfer is a struct that holds all the required fields for the WAL to hold a transfer request entry...
type WALTransfer struct {
	Source            string
	Target            string
	Amount            int64
	IdempotencyKey string
}

// WALInit is a struct that holds all the required fields for the WAL to hold an initialise account request entry...
type WALInit struct {
	Username        string
	StartingBalance int64
}

// FileWAL is a file-based implementation of the WAL, append-only to the global.wal file.
type FileWAL struct {
	File *os.File
	path string
}

// NewFileWAL is a constructor for FileWAL.
func NewFileWAL(path string) (*FileWAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileWAL{
		File: file,
		path: path,
	}, nil
}

// Append is the append method for FileWAL.
func (wal *FileWAL) Append(entry *WALEntry) error {
	// 1. Turn the entry into JSON bytes...
	bytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	// 2. Add a new line to each entry...
	bytes = append(bytes, '\n')

	// 3. Write bytes to the file... (only reaches OS buffer).
	if _, err := wal.File.Write(bytes); err != nil {
		return err
	}
	// 4. fsync: force the OS to flush to physical disk I WANT NOW.
	return wal.File.Sync()
}

// Replay is the replay method for FileWAL.
func (wal *FileWAL) Replay() ([]WALEntry, error) {
	// Open read-only handle...
	file, err := os.Open(wal.path)
	if err != nil {
		// File doesn't exist yet.. Return zero entries if so...
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var entries []WALEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry WALEntry
		// Unmarshal each line back into a WALEntry...
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			// A line failed to parse...
			return entries, fmt.Errorf("corrupt WAL entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return entries, err
	}
	return entries, nil
}
