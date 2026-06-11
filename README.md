# Concurrent Bank Ledger

A Go project exploring concurrency primitives through a simple banking domain. Built to compare two synchronisation strategies: fine-grained mutexes vs. a single actor goroutine.

---

## What it does

A gRPC server that exposes two operations:

- **InitialiseAccount** — creates an account and funds it from a system mint via a genesis transfer
- **Transfer** — moves an amount between two accounts, with idempotency protection against duplicate requests

All state lives in memory. There is no database.

---

## Architecture

```
gRPC client
    │  proto request (HTTP/2)
    ▼
gRPC server (server.go)
    │  wraps request into LedgerCommand struct
    │  sends onto buffered channel (cap: 10,000)
    │  then blocks on a per-request ReplyTo channel
    ▼
Buffered channel (queue)
    │  if full, the gRPC handler goroutine stalls here
    ▼
Actor loop (single goroutine, started in main.go)
    │  drains commands one at a time
    │  executes pure state mutations — no locks needed
    │  sends result back on the ReplyTo channel
    ▼
In-memory ledger (Ledger struct)
    └── Accounts map, LedgerHistory slice, AttemptedTransactions map
```

The key tradeoff: because all mutations run through one goroutine, no two transfers can execute in parallel — even if they touch completely different accounts. This eliminates lock contention entirely but caps throughput at what a single core can process.

---

## The two phases

### Phase 1 — Fine-grained mutexes (`ledger_mutex.go`)

Each `Account` embeds a `sync.Mutex`. The `Ledger` itself has a separate mutex protecting shared structures (the accounts map, ledger history, idempotency records).

To prevent deadlock when locking two accounts simultaneously, locks are always acquired in alphabetical order:

```go
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
```

This guarantees a consistent lock ordering regardless of which direction the transfer runs.

### Phase 2 — Actor model (`ledger_actor.go`)

A single background goroutine owns all state. External callers send `LedgerCommand` structs down a channel and block on a per-request reply channel:

```go
// Caller side (gRPC handler)
replyChan := make(chan TransferResponse, 1)
queue <- LedgerCommand{Type: "TRANSFER", Transfer: &TransferRequest{..., ReplyTo: replyChan}}
result := <-replyChan  // blocks until actor responds

// Actor side
for cmd := range queue {
    switch cmd.Type {
    case "TRANSFER":
        success, err := ledger.executePureTransfer(...)
        cmd.Transfer.ReplyTo <- TransferResponse{success, err}
    }
}
```

Because only one goroutine ever touches the state, no locks are needed inside `executePureTransfer` or `executePureInitialise`.

---

## Benchmarks

Measured on AMD Ryzen 7 8845HS (Zen 4).

| Strategy | Scenario | Cores | ns/op | B/op |
|---|---|---|---|---|
| Mutex | Low contention | 1 | 2667 | 796 |
| Mutex | Low contention | 8 | 2400 | 868 |
| Mutex | High contention | 8 | 2522 | 940 |
| Actor | Low contention | 1 | 2347 | 961 |
| Actor | Low contention | 4 | 3596 | 934 |
| Actor | High contention | 8 | 2462 | 988 |

The actor model gets slower under low contention on multiple cores because all goroutines are serialising through a single channel — the extra coordination overhead outweighs the benefit of eliminating lock contention. Under high contention the gap closes, because mutex thrashing starts to cost more than the serialisation does.

---

## Known limitations

- **No persistence** — all state is lost on restart
- **Actor throughput ceiling** — every operation, regardless of which accounts it touches, queues behind every other operation
- **`LedgerCommand.Type` is a string** — a typo silently drops a command with no error
- **`context.Context` is ignored** in gRPC handlers — client disconnects cannot cancel in-flight operations
- **Errors are not wrapped** in some places, making production debugging harder than it needs to be

---

## Running it

```bash
go mod tidy
go build -o bankserver main.go
./bankserver
```

To test via a browser UI:

```bash
go install github.com/fullstorydev/grpcui/cmd/grpcui@latest
grpcui -plaintext -proto api/proto/ledgerapi/ledger.proto 127.0.0.1:8080
```

---

## Project layout

```
├── main.go                          # Entry point, wires up channel, actor loop, gRPC server
├── internal/
│   ├── domain/
│   │   ├── models.go                # Structs: Account, Ledger, Transaction, LedgerCommand
│   │   ├── ledger_mutex.go          # Phase 1: mutex-based Transfer and InitialiseAccount
│   │   └── ledger_actor.go          # Phase 2: lock-free actor loop and pure mutation functions
│   └── ports/grpc/
│       └── server.go                # gRPC adapter: maps proto requests to domain commands
└── api/proto/ledgerapi/             # Proto definitions and generated stubs
```
