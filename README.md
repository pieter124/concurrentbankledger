# Concurrent Bank Ledger

A Go project exploring concurrency strategies through a simple banking domain. It compares three
approaches to safely mutating shared financial state under concurrent load:

1. **Fine-grained mutexes** — per-account locks with deadlock-free ordering
2. **Single actor** — one goroutine owns all state; callers send commands over a channel
3. **Sharded actors** — state partitioned across N actors, each owning a disjoint slice of accounts,
   with a two-phase protocol for transfers that cross a shard boundary

All state lives in memory. There is no database.

---

## What it does

A gRPC server exposing two operations:

- **InitialiseAccount** — creates an account and funds it from a system mint via a genesis transfer
- **Transfer** — moves an amount between two accounts, with idempotency protection against duplicate requests

Money is represented as integer pence; balances are derived by summing an account's transaction history,
so a transfer is never an in-place edit but an append of mirrored debit/credit legs (double-entry).

---

## The three phases

### Phase 1 — Fine-grained mutexes (`ledger_mutex.go`)

Each `Account` embeds a `sync.Mutex`; a separate ledger mutex guards the shared maps. To avoid deadlock
when a transfer locks two accounts, locks are always taken in a fixed (alphabetical) order, so two
opposing transfers can never each hold what the other wants.

### Phase 2 — Single actor (`ledger_actor.go`)

One background goroutine owns all state. Callers send a `LedgerCommand` down a channel and block on a
per-request reply channel. Because only one goroutine ever touches the state, the mutation functions need
no locks at all.

The tradeoff this exposes: every operation queues behind every other, even transfers touching completely
unrelated accounts. Lock contention is gone, but throughput is capped at one core.

### Phase 3 — Sharded actors (`ledger_actor.go`, routed in `server.go`)

Phase 3 lifts the single-actor ceiling. Accounts are partitioned across N independent ledgers by
`fnv(account) % N`, each with its own actor goroutine. Every account lives in exactly one shard — there
is no replication — so each actor still owns its state outright and runs lock-free.

This splits transfers into two cases:

- **Intra-shard** (source and target hash to the same shard): one actor owns both accounts, so it runs
  the whole transfer in a single command — the fast path.
- **Cross-shard** (different shards): no single actor can see both accounts, so the gRPC handler acts as a
  coordinator and runs a two-phase sequence:
  `RESERVE` (debit + hold on the source shard) → `CREDIT` (add on the target shard) →
  `FINALIZE` (mark the held transfer complete). If the credit fails, `REFUND` posts a compensating entry
  on the source shard so no money is lost.

The coordinator lives in the caller, never inside an actor — an actor must never block its own loop
waiting on another actor, or two cross-shard transfers could deadlock each other. The caller is allowed
to block, so the coordination cycle is safe there.

---

## Architecture

```
gRPC client
    │  proto request (HTTP/2)
    ▼
gRPC server (server.go)
    │  hash(source), hash(target) -> pick shard(s)
    │  same shard  -> one TRANSFER command
    │  diff shards -> RESERVE -> CREDIT -> FINALIZE (REFUND on failure)
    ▼
N buffered channels (one queue per shard)
    ▼
N actor loops (one goroutine per shard)
    │  each drains its own queue, mutates only its own ledger, lock-free
    ▼
N in-memory ledgers (disjoint partitions of the accounts)
```

---

## Benchmarks

Measured on an **AMD Ryzen 7 8845HS** (Zen 4), 8 shards, swept across 1, 4, and 8 cores.
`ns/op` is per transfer (lower is faster).

**Low contention** — random transfers across a wide pool of accounts:

| Strategy      | 1 core | 4 cores | 8 cores | B/op | allocs/op |
|---------------|--------|---------|---------|------|-----------|
| Mutex         | 4326   | 2442    | 1978    | 840  | 8         |
| Single actor  | 3107   | 2506    | 2153    | 942  | 9         |
| Sharded (8)   | 3740   | 2362    | **1632**| 1388 | 17        |

**High contention** — every transfer targets one hot account ("The Mint"):

| Strategy      | 1 core | 4 cores | 8 cores | B/op | allocs/op |
|---------------|--------|---------|---------|------|-----------|
| Mutex         | 1846   | 1883    | 1866    | 885  | 6         |
| Single actor  | 2471   | 2223    | 2056    | 957  | 7         |
| Sharded (8)   | 3465   | 2311    | 1744    | 1433 | 15        |

What the numbers show:

- **Sharding wins when load spreads.** At 8 cores under low contention it's the fastest strategy
  (1632 ns/op), because work fans out across 8 actors on 8 cores.
- **The single actor barely scales with cores** (3107 → 2153, ~1.4×) — it can't, since everything
  serialises through one goroutine. Mutex and sharded both scale ~2.3×. That gap *is* the bottleneck
  Phase 3 was built to remove.
- **A hot key erases sharding's edge.** When every transfer targets one account, all credits funnel into
  that account's single shard, recreating a single-actor bottleneck — and sharding pays a cross-shard tax
  on top (note the higher allocs/op). Sharding helps in proportion to how well traffic distributes.

---

## Correctness

The core invariant is conservation of money (zero-sum). A test fires thousands of concurrent randomized
transfers across many accounts and asserts the total is unchanged afterward, run under `go test -race` to
catch data races as well as lost updates:

```bash
go test -race -count=20 ./internal/domain/
```

---

## Known limitations

- **No persistence** — all state is lost on restart. The RESERVE→CREDIT→FINALIZE record is the natural
  thing a write-ahead log would persist; that's the intended next step.
- **Hot-key bottleneck** — a single very popular account serialises all of its credits through one shard,
  regardless of shard count (see benchmarks).
- **Cross-shard cost** — a cross-shard transfer is ~3 messages instead of 1, roughly doubling allocations
  per op; it is also briefly non-atomic (money is debited from the source before it lands on the target,
  protected by the pending idempotency state during that window).
- **`context.Context` is ignored** in handlers — client disconnects can't cancel in-flight work.

---

## Running it

```bash
go mod tidy
go build -o bankserver main.go
./bankserver
```

Test via a browser UI:

```bash
go install github.com/fullstorydev/grpcui/cmd/grpcui@latest
grpcui -plaintext -proto api/proto/ledgerapi/ledger.proto 127.0.0.1:8080
```

---

## Project layout

```
├── main.go                          # Wires up N queues + N shard ledgers + N actor loops, runs gRPC server
├── internal/
│   ├── domain/
│   │   ├── models.go                # Structs: Account, Ledger, commands, requests/responses
│   │   ├── ledger_mutex.go          # Phase 1: mutex-based Transfer / InitialiseAccount
│   │   └── ledger_actor.go          # Phase 2 & 3: pure mutations, reserve/credit/refund/finalize handlers
│   └── ports/grpc/
│       └── server.go                # gRPC adapter + cross-shard coordinator (routeTransfer)
└── api/proto/ledgerapi/             # Proto definitions and generated stubs
```
