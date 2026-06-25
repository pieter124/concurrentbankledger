package domain

import (
	"os"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	// Initialise WAL...
	path := t.TempDir() + "/test.wal"
	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("could not make a test.wal file...")
	}

	// Add transfer entry...
	transfer := &WALTransfer{
		Source:         "alice",
		Target:         "bob",
		Amount:         1,
		IdempotencyKey: "testing123",
	}
	err = wal.Append(&WALEntry{
		CommandType:           TransferCommand,
		TransferInfo:          transfer,
		InitialiseAccountInfo: nil,
	})
	if err != nil {
		t.Fatalf("could not append transfer WAL entry...")
	}

	// Add init entry...
	initialiseAcc := &WALInit{
		Username:        "user",
		StartingBalance: 12345,
	}
	err = wal.Append(&WALEntry{
		CommandType:           InitialiseAccountCommand,
		TransferInfo:          nil,
		InitialiseAccountInfo: initialiseAcc,
	})
	if err != nil {
		t.Fatalf("could not append init WAL entry...")
	}

	// Replay WAL....
	var entries []WALEntry
	entries, err = wal.Replay()
	if err != nil {
		t.Fatalf("could not replay wal...")
	}

	// verify entries...
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Check first entry (transfer)...
	if entries[0].CommandType != TransferCommand {
		t.Fatalf("entry 0 should be transfer, got type %d", entries[0].CommandType)
	}
	if entries[0].TransferInfo == nil {
		t.Fatalf("entry 0 transfer info is nil")
	}
	if entries[0].TransferInfo.Source != "alice" ||
		entries[0].TransferInfo.Target != "bob" ||
		entries[0].TransferInfo.Amount != 1 ||
		entries[0].TransferInfo.IdempotencyKey != "testing123" {
		t.Fatalf("entry 0 transfer info fields wrong: %+v", entries[0].TransferInfo)
	}

	// Check second entry (init)...
	if entries[1].CommandType != InitialiseAccountCommand {
		t.Fatalf("entry 1 should be an init, got type %d", entries[1].CommandType)
	}
	if entries[1].InitialiseAccountInfo == nil {
		t.Fatalf("entry 1 init info is nil")
	}
	if entries[1].InitialiseAccountInfo.Username != "user" ||
		entries[1].InitialiseAccountInfo.StartingBalance != 12345 {
		t.Fatalf("entry 1 init fields wrong: %+v", entries[1].InitialiseAccountInfo)
	}
}

func TestPersistAcrossInstances(t *testing.T) {
	// Initialise WAL...
	path := t.TempDir() + "/test.wal"
	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("could not make a test.wal file...")
	}

	// Add transfer entry...
	transfer := &WALTransfer{
		Source:         "alice",
		Target:         "bob",
		Amount:         1,
		IdempotencyKey: "testing123",
	}
	err = wal.Append(&WALEntry{
		CommandType:           TransferCommand,
		TransferInfo:          transfer,
		InitialiseAccountInfo: nil,
	})
	if err != nil {
		t.Fatalf("could not append transfer WAL entry...")
	}

	// Add init entry...
	initialiseAcc := &WALInit{
		Username:        "user",
		StartingBalance: 12345,
	}
	err = wal.Append(&WALEntry{
		CommandType:           InitialiseAccountCommand,
		TransferInfo:          nil,
		InitialiseAccountInfo: initialiseAcc,
	})
	if err != nil {
		t.Fatalf("could not append init WAL entry...")
	}

	// Replay WAL from a diff FileWAL to verify it persists on disk...
	var entries []WALEntry
	newWal, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("could not open new wal with same path...")
	}

	entries, err = newWal.Replay()
	if err != nil {
		t.Fatalf("could not replay wal with new instance...")
	}

	// verify entries...
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Check first entry (transfer)...
	if entries[0].CommandType != TransferCommand {
		t.Fatalf("entry 0 should be transfer, got type %d", entries[0].CommandType)
	}
	if entries[0].TransferInfo == nil {
		t.Fatalf("entry 0 transfer info is nil")
	}
	if entries[0].TransferInfo.Source != "alice" ||
		entries[0].TransferInfo.Target != "bob" ||
		entries[0].TransferInfo.Amount != 1 ||
		entries[0].TransferInfo.IdempotencyKey != "testing123" {
		t.Fatalf("entry 0 transfer info fields wrong: %+v", entries[0].TransferInfo)
	}

	// Check second entry (init)...
	if entries[1].CommandType != InitialiseAccountCommand {
		t.Fatalf("entry 1 should be an init, got type %d", entries[1].CommandType)
	}
	if entries[1].InitialiseAccountInfo == nil {
		t.Fatalf("entry 1 init info is nil")
	}
	if entries[1].InitialiseAccountInfo.Username != "user" ||
		entries[1].InitialiseAccountInfo.StartingBalance != 12345 {
		t.Fatalf("entry 1 init fields wrong: %+v", entries[1].InitialiseAccountInfo)
	}
}

func TestReplayCorruptLine(t *testing.T) {
	path := t.TempDir() + "/corrupt.wal"

	// Write one valid entry through the WAL.
	wal, err := NewFileWAL(path)
	if err != nil {
		t.Fatalf("could not create wal: %v", err)
	}
	err = wal.Append(&WALEntry{
		CommandType: TransferCommand,
		TransferInfo: &WALTransfer{
			Source:         "alice",
			Target:         "bob",
			Amount:         1,
			IdempotencyKey: "q1",
		},
	})
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Simulate a crash mid write, append a broken json file directly to the file...
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("could not open file to corrupt it: %v", err)
	}
	file.WriteString(`{"CommandType":0,"TransferInfo:{"Source":"al`)
	defer file.Close()

	// Replay must detect corruption and return an error...
	_, err = wal.Replay()
	if err == nil {
		t.Fatalf("expected an error from the corrupt line, got nil")
	}
}
