# Concurrent Bank Ledger

A high-performance, thread-safe core banking ledger written in Go. This engine handles atomic asset transfers using double-entry bookkeeping principles, ensuring zero-sum enforcement across concurrent operations without risking deadlocks, race conditions, or data corruption.

Originally built as an isolated simulation, the system has been evolved into a **live, production-grade gRPC Microservice** capable of handling ultra-fast binary connection streams over HTTP/2.

---

## 🚀 Key Architectural Features

* **Double-Entry Tracking (Zero-Sum):** Capital is never simply incremented or decremented in isolation. Every transfer generates a global ledger log alongside symmetrical local transaction balances (debits and credits).
* **Enforced Lock Ordering (Deadlock Prevention):** Implements fine-grained locking on individual account resources. To safely allow bidirectional high-velocity transfers (e.g., Alice → Bob and Bob → Alice simultaneously), the engine compares account keys lexicographically to guarantee an unchanging, deterministic locking order.
* **Optimized Memory Strategy:** Utilizes explicit slice capacity pre-allocation to prevent inefficient background memory re-allocations during high-frequency append storms.
* **Encapsulated Synchronization:** Leverages Go struct embedding (anonymous mutexes) to maintain a highly readable API without cluttering core business models.
* **Language-Agnostic gRPC Interface:** Exposes its core engine over an ultra-low latency HTTP/2 protocol layer using Protocol Buffers (`proto3`), bypassing heavy and slow JSON text parsing.

---

## Code Layout Overview

The codebase implements a decoupled, hexagonal architecture (Ports and Adapters):

```text
ConcurrentBankLedger/
├── api/proto/ledgerapi/   # Language-agnostic Protocol Buffer definitions & auto-generated code
├── internal/
│   ├── domain/            # Pure, isolated business logic (Locks, Maps, Double-entry verification)
│   └── ports/grpc/        # Network Adapter (Intercepts gRPC binary requests and maps them to Domain)
└── main.go                # Application Entry Point & Dependency Injection bootstrapping
Core Architecture Components
Models (internal/domain): Contains the Transaction master records (storing amounts as int64 cents to prevent floating-point rounding errors), localized journal logs, and the central Ledger coordinator.

Network Protocol (api/proto): The definitive service contract file (ledger.proto) used to generate optimized Go wire-parsing structs.

gRPC Adapter (internal/ports/grpc): Custom handwriting wrapper embedding UnimplementedLedgerServiceServer to satisfy strict interface composition rules while safely guaranteeing forward-compatibility.

⚡ The API Contract (ledger.proto)
The entire system communication is bound to a strict, compile-time contract:

Protocol Buffers
syntax = "proto3";

package ledgerapi;
option go_package = "concurrent-bank-ledger/api/proto/ledgerapi";

service LedgerService {
  rpc Transfer (TransferRequest) returns (TransferResponse);
}

message TransferRequest {
  string source = 1;
  string target = 2;
  int64 amount = 3;
  string idempotency_key = 4;
}

message TransferResponse {
  bool success = 1;
  string message = 2;
}
💻 Getting Started & Running the Server
Prerequisites
Go 1.22+

Protocol Buffer Compiler (protoc) installed on your local machine

Installation & Compilation
Clone the repository and download the required external core packages (Google gRPC and Google UUID modules):

Bash
go get google.golang.org/grpc
go get [github.com/google/uuid](https://github.com/google/uuid)
go mod tidy
Compile the entire project down into a single, optimized machine-code binary:

Bash
go build -o bankserver main.go
Launch your live backend banking network:

Bash
./bankserver
The server will boot up, seed genesis balances (alice: 10000, bob: 5000), and securely begin listening for network traffic on port :8080.

🔍 Interactive Live Testing
Because gRPC communicates using compressed, raw binary data, traditional HTTP web browsers or curl text commands cannot interface with it directly.

To test your service live with an interactive Web Graphical Interface (similar to Postman/Swagger), you can utilize grpcui:

Install the tool globally on your system:

Bash
go install [github.com/fullstorydev/grpcui/cmd/grpcui@latest](https://github.com/fullstorydev/grpcui/cmd/grpcui@latest)
Point it directly to your running server and your local contract blueprint file:

Bash
grpcui -plaintext -proto api/proto/ledgerapi/ledger.proto 127.0.0.1:8080
Open the link generated in your terminal to invoke real-time transactions (e.g., executing a transfer from alice to bob), stress-test insufficient balance rejections, or observe how the engine handles duplicate idempotency tokens seamlessly across the wire.
