# Concurrent Bank Ledger

A Go project exploring concurrency and durability through a simple banking domain. It implements and
compares three strategies for safely mutating shared financial state under concurrent load, then adds a
write-ahead log so that state survives a crash.

1. **Fine-grained mutexes** — per-account locks with deadlock-free ordering
2. **Single actor** — one goroutine owns all state; callers send commands over a channel
3. **Sharded actors** — state partitioned across N actors, each owning a disjoint slice of accounts,
   with a two-phase protocol for transfers that cross a shard boundary

A **write-ahead log (WAL)** persists every committed operation to disk and replays it on startup, so the
ledger recovers its full state after a restart or crash.

---

## What it does

A gRPC server exposing two operations:

- **InitialiseAccount** — creates an account and funds it from a system mint via a genesis transfer
- **Transfer** — moves an amount between two accounts, with idempotency protection against duplicates

Money is integer pence; balances are derived by summing an account's transaction history, so a transfer
is never an in-place edit but an append of mirrored debit/credit legs (double-entry).

---

## The three concurrency phases

### Phase 1 — Fine-grained mutexes
Each account has its own lock; a transfer takes both locks in a fixed (alphabetical) order so two
opposing transfers can never deadlock each other.

### Phase 2 — Single actor
One goroutine owns all state; callers send a command down a channel and block on a reply channel. No locks
are needed because only one goroutine ever touches the state. The cost: every operation queues behind
every other, so throughput is capped at one core.

### Phase 3 — Sharded actors
Accounts are partitioned across N independent ledgers by `fnv(account) % N`, each with its own actor
goroutine. Every account lives in exactly one shard (no replication), so each actor still owns its state
outright and runs lock-free. Transfers split into two cases:

- **Intra-shard** — one actor owns both accounts; the whole transfer runs as a single command (fast path).
- **Cross-shard** — no single actor sees both accounts, so the caller coordinates a two-phase sequence:
  `RESERVE` (debit + hold on the source shard) → `CREDIT` (add on the target shard) → `FINALIZE`.
  If the credit fails, `REFUND` posts a compensating entry on the source shard so no money is lost. The
  coordinator lives in the caller, never inside an actor — an actor must never block waiting on another
  actor, or two cross-shard transfers could deadlock.

The system mint lives in exactly one shard (by hash of its name); genesis funding therefore routes
cross-shard like any other transfer, using the same reserve/credit/finalize path.

---

## Durability — the Write-Ahead Log

Phase 3 made the ledger fast and parallel but still lost everything on restart. The WAL fixes that.

- Each committed operation (a transfer or an account initialisation) is appended to a single global log
  file as one newline-delimited JSON record, and **fsync'd to disk** before the response is returned.
- On startup, the server **replays** the log top-to-bottom through the same routing logic the live path
  uses, rebuilding every account and balance before it accepts any traffic.
- Replay applies operations *below* the logging layer, so re-running the log never re-logs — the file
  doesn't grow on every restart.
- A corrupt or truncated final record (the expected shape after a crash mid-write) is detected on replay,
  and the server refuses to start with incomplete state rather than serving wrong balances.

**Verified:** initialise accounts, run transfers, `kill` the process, restart — the accounts and balances
come back exactly as they were, rebuilt entirely from the log.

This is a **log-after-commit** WAL: an operation is applied and then logged. A true write-ahead log logs
the *intent* first and records the outcome separately; the simplification here is noted under Limitations.

---

## Benchmarks

Measured on an **AMD Ryzen 7 8845HS** (8 cores / 16 threads, Zen 4), using all 16 threads,
median of 5 runs. `ns/op` per transfer, lower is faster.

| Strategy      | Low contention | High contention |
|---------------|----------------|-----------------|
| Mutex         | ~1450          | ~1880           |
| Single actor  | ~2225          | ~1930           |
| Sharded (8)   | **~1090**      | ~1140           |

- **Sharding is fastest under both workloads** — ~1090 ns/op, roughly 2× faster than the single
  actor and ~25% faster than fine-grained mutexes, because work fans out across 8 actor goroutines
  instead of serialising through one.
- **The single actor is slowest under low contention** — every operation queues behind every other
  through a single goroutine, so additional cores can't speed up the core work.
- **Sharding holds up under a hot key** — when every transfer targets one account ("The Mint"),
  sharded throughput barely changes (~1090 → ~1140). Credits funnel into the mint's single shard, but
  reserves still parallelise across shards, so the cross-shard coordination cost stays modest at this
  scale. (A more extreme hot-key workload would eventually expose the single-shard ceiling on the hot
  account.)

---

## Correctness

Zero-sum conservation under concurrency, checked with the race detector:

```bash
go test -race -count=20 ./internal/domain/
```

The WAL has its own tests: round-trip (append → replay → fields/order intact), persistence across separate
WAL instances on the same file (proving on-disk durability), and corrupt-line detection.

---

## Known limitations

- **No log compaction.** The WAL grows unboundedly; a production system would periodically snapshot state and truncate the log.
- **Single global log fsync.** All shards serialise through one log file's fsync; a high-throughput design would shard the log (and reconcile ordering on replay) or batch fsyncs via group commit.
- **Hot-key bottleneck.** A single very popular account serialises its credits through one shard, regardless of shard count.

---

## Running it

```bash
go mod tidy
go build -o bankserver main.go
./bankserver
```

State is persisted to `ledger.wal` and replayed automatically on the next start.

Test via a browser UI:

```bash
go install github.com/fullstorydev/grpcui/cmd/grpcui@latest
grpcui -plaintext -proto api/proto/ledgerapi/ledger.proto 127.0.0.1:8080
```

---

## Project layout

```
├── main.go                          # Wires N queues + N shard ledgers + actor loops, opens WAL, runs server
├── internal/
│   ├── domain/
│   │   ├── models.go                # Account, Ledger, commands, WAL entry types
│   │   ├── ledger_mutex.go          # Phase 1: mutex-based transfer / init
│   │   ├── ledger_actor.go          # Phase 2 & 3: pure mutations, reserve/credit/refund/finalize
│   │   └── wal.go                   # WAL interface + FileWAL (append + fsync, replay)
│   └── ports/grpc/
│       └── server.go                # gRPC adapter, cross-shard coordinator, WAL logging + recovery
└── api/proto/ledgerapi/             # Proto definitions and generated stubs
```

## What I learnt
This started as a comparison of concurrency strategies, which grew into a small distributed systems project.

**Concurrency is a trade-off between contention and parallelism**. Fine-grained mutexes parallelise well with a cost of lock contention, a single actor eliminates contention by giving one thread (goroutine in this case) sole ownership of all state, but caps throughput at one core. Sharded actors improve the parallelism whilst eliminating lock contention by partitioning state, with the cost of increased complexity (as per the two-phase protocol for cross-shard operations).

**Cross-shard atomicity requires a commit protocol**. When a transfer's two accounts live on different shards, no single actor can access both, and so the operation cannot be atomic in one step. I built a reserve-credit-finalize protocol (with a correctly compensated refund on failure) so that money is never lost or duplicated even if the transfer fails halfway through the request. It first reserves the money which the source account is going to transfer. If that is successful, it then sends the money to the transfer account. If the second operation fails, we refund the source account with the money, else we do a finalize operation to mark the transfer as complete.

**Durability is about ordering and fsync, not just writing to a file**. A WAL only protects you if the write record reaches the physical disk. Write alone leaves the data in an OS buffer that a crash can wipe, so fsync guarantees durability of that operation. I made the WAL simple, making it a log-after-commit WAL, rather than the traditional WAL which records intent aswell.

**Tests and benchmarks are how to trust code (particularly concurrent code!)**. Tests including zero-sum invariance ran under Go's race detector caught problems that ordinary tests would miss, the benchmarks confirmed performance differences I expected, and also flagged a result that was too fast to be real, which turned out to be transfers getting silently rejected, rather than executed.

## What I would do differently moving forward

- **True write-ahead ordering**. Log the intent first, so a crash in the window between applying and logging cannot lose an operation.
- **Log compaction**. Snapshot the state periodically and truncating the log so that it doesn't grow forever and replays stay quick on startup. We can also batch fsync operations to amortise the per-write disk cost and improve write latency.
- **Handle context.Context in handlers**. So that a client's disconnect can cancel the in-flight request.
