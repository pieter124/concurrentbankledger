# Concurrent Bank Ledger

A high-performance, thread-safe core banking engine written in Go. This system handles atomic asset transfers using double-entry bookkeeping principles, ensuring strict zero-sum enforcement across concurrent operations without risking deadlocks, data races, or state corruption.

Originally built as an isolated domain simulation, the project has evolved into a production-grade, lock-free gRPC microservice capable of handling low-latency binary streams over HTTP/2.

---

## Architectural Evolution & Benchmarks (The Case Study)

The core engine was benchmarked across two distinct synchronization paradigms under both low and extreme account contention on modern Zen 4 hardware (**AMD Ryzen 7 8845HS**). 

### Benchmark Performance Matrix

| Synchronization Strategy | Concurrency Scenario | Core Count | Throughput Execution Speed | Memory Allocation |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 1: Fine-Grained Mutex** | Low Contention | 1 Core | **2667 ns/op** | 796 B/op (8 allocs) |
| **Phase 1: Fine-Grained Mutex** | Low Contention | 8 Cores | **2400 ns/op** | 868 B/op (8 allocs) |
| **Phase 1: Fine-Grained Mutex** | High Contention | 8 Cores | **2522 ns/op** | 940 B/op (6 allocs) |
| **Phase 2: Single-Channel Actor** | Low Contention | 1 Core | **2347 ns/op** | 961 B/op (9 allocs) |
| **Phase 2: Single-Channel Actor** | Low Contention | 4 Cores | **3596 ns/op** | 934 B/op (9 allocs) |
| **Phase 2: Single-Channel Actor** | High Contention | 8 Cores | **2462 ns/op** | 988 B/op (7 allocs) |

### Architectural Insights
* **Phase 1 (Mutexes Protect State):** Uses alphabetical lexicographical key sorting to guarantee a deterministic locking order and prevent deadlocks. Scales efficiently across multiple CPU cores during low-contention scenarios, but encounters performance degradation under high contention due to CPU thread-parking and lock thrashing.
* **Phase 2 (Channels Orchestrate Work):** Eliminates mutual exclusion flags entirely by isolating core state inside a single background actor loop. This model neutralizes lock contention on highly active accounts but introduces a processing bottleneck on a single CPU core when high numbers of worker threads attempt to access the centralized pipeline.

---

## Key Technical Features

* **Double-Entry Tracking (Zero-Sum):** Assets are never incremented or decremented in isolation. Every transfer creates an immutable global ledger entry alongside balanced, mirrored local account journal updates (debits/credits).
* **Idempotency Guarantee:** Protects against distributed system retry storms. Leverages an atomic tracking registry to detect duplicate processing keys, short-circuiting re-executed payloads with original cached results.
* **Optimized Memory Footprint:** Utilizes explicit slice capacity pre-allocation to eliminate the overhead of dynamic heap re-allocations during high-frequency transaction append cycles.
* **Decoupled Architecture:** Follows Clean/Hexagonal principles, keeping core business logic (`internal/domain`) isolated from network adapters (`internal/ports/grpc`).
* **High-Performance Transport Layer:** Replaces standard JSON text parsing with an ultra-low latency HTTP/2 protocol layer powered by Google Protocol Buffers (`proto3`).

---

## Code Layout

```text
ConcurrentBankLedger/
├── api/proto/ledgerapi/   # Protocol Buffer definitions & auto-generated wire stubs
├── internal/
│   ├── domain/            # Pure domain layer (models.go, ledger_mutex.go, ledger_actor.go)
│   └── ports/grpc/        # Network Adapter (gRPC server implementation & payload mapping)
└── main.go                # System entry point, lifecycle management, & dependency injection
```text
```

## Getting Started
Prerequisites

    Go 1.22+

    Protocol Buffer Compiler (protoc) installed locally

Installation & Compilation

    Clone the repository and fetch the required core modules:
    Bash

    go get google.golang.org/grpc
    go get [github.com/google/uuid](https://github.com/google/uuid)
    go mod tidy

    Compile the system down into an optimized native machine-code binary:
    Bash

    go build -o bankserver main.go

    Launch your backend banking node:
    Bash

    ./bankserver

    The server will initialize internal state, seed genesis account structures, and begin listening for incoming HTTP/2 gRPC traffic on port :8080.

Interactive Live Testing

Because gRPC communicates using compressed, raw binary streams, traditional HTTP text utilities like curl cannot interface with it directly. Use grpcui to test the microservice interactively via a local Web Graphical Interface.

    Install the tool globally on your system:
    Bash

    go install [github.com/fullstorydev/grpcui/cmd/grpcui@latest](https://github.com/fullstorydev/grpcui/cmd/grpcui@latest)

    Bind it directly to your active engine instance and contract specification file:
    Bash

    grpcui -plaintext -proto api/proto/ledgerapi/ledger.proto 127.0.0.1:8080

    Open the browser link outputted in your terminal to dispatch live transactions, inspect insufficient funds handler branches, and execute concurrent idempotency key checks across the wire.
