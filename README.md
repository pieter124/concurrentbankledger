# Concurrent Bank Ledger

A high-performance, thread-safe core banking ledger written in Go. This engine handles atomic asset transfers using double-entry book-keeping principles, ensuring zero-sum enforcement across concurrent operations without risking deadlocks or data corruption.

## 🚀 Key Architectural Features

* **Double-Entry Tracking (Zero-Sum):** Capital is never simply incremented or decremented. Every transfer generates a global ledger log alongside symmetrical local transaction balances (debits and credits).
* **Enforced Lock Ordering (Deadlock Prevention):** Implements fine-grained locking on individual account resources. To safely allow bidirectional high-velocity transfers (e.g., Alice $\rightarrow$ Bob and Bob $\rightarrow$ Alice simultaneously), the engine compares account keys lexicographically to guarantee an unchanging locking order.
* **Optimized Memory Strategy:** Utilizes explicit slice capacity pre-allocation to prevent inefficient background memory re-allocations during high-frequency append storms.
* **Encapsulated Synchronization:** Leverages Go struct embedding (anonymous mutexes) to maintain a highly readable API without cluttering core business models.

---

## 🛠️ Code Layout Overview

The codebase represents a self-contained domain engine executing the following primary layers:

### Models
* `Transaction`: The global master record containing unique transaction UUIDs, source/target identities, timestamps, and amounts in pence to prevent floating-point rounding errors.
* `LocalAccountTransaction`: Compact, localized journal logs optimized for rapid local account balance derivation.
* `Account`: An isolated user context equipped with an embedded mutex to shield its transactional history.
* `Ledger`: The central coordinator managing account registries and global master history.

---

## 💻 Running the Simulation

The `main()` file seeds a genesis balance from a secure system vault (`"The Mint"`) and spins up 50 parallel goroutines firing simultaneous cross-transfers to rigorously stress-test the synchronization guarantees.

### Prerequisites

Ensure you have a modern Go runtime installed and fetch the UUID library module:

```bash
go get [github.com/google/uuid](https://github.com/google/uuid)
