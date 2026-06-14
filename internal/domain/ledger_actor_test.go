package domain

import "testing"

func TestReserveHoldsFunds(t *testing.T) {
	ledger := InitialiseLedger()
	_ = ledger.executePureInitialise("alice", 1000)

	before := ledger.Account["alice"].GetBalance()
	res := ledger.executeReserve("alice", "bob", 300, "tx_reserve_test")

	if !res.Proceed || res.Err != nil {
		t.Fatalf("expected a fresh reservation, got %+v", res)
	}
	after := ledger.Account["alice"].GetBalance()
	if before-after != 300 {
		t.Fatalf("expected source to drop by 300, dropped by %d", before-after)
	}
	if res.TxnID == "" {
		t.Fatalf("expected a txn id to be minted")
	}
}

// Credit adds money to the target and is the only place target-existence is checked.
func TestCreditLandsFunds(t *testing.T) {
	ledger := InitialiseLedger()
	_ = ledger.executePureInitialise("bob", 500)

	ok, err := ledger.executeCredit("bob", 300, "txn-123")
	if !ok || err != nil {
		t.Fatalf("expected credit to succeed, got ok=%v err=%v", ok, err)
	}
	if bal := ledger.Account["bob"].GetBalance(); bal != 800 {
		t.Fatalf("expected bob at 800, got %d", bal)
	}
}

func TestCreditRejectsMissingTarget(t *testing.T) {
	ledger := InitialiseLedger() // no "ghost" account created
	ok, err := ledger.executeCredit("ghost", 300, "txn-123")
	if ok || err == nil {
		t.Fatalf("expected credit to a missing target to fail, got %v %v", ok, err)
	}
}

// The full flow of a cross-shard transfer.
func TestReserveCreditFinalizeFlow(t *testing.T) {
	src := InitialiseLedger()
	dst := InitialiseLedger()
	_ = src.executePureInitialise("alice", 1000)
	_ = dst.executePureInitialise("bob", 500)

	res := src.executeReserve("alice", "bob", 300, "tx_flow")
	if !res.Proceed || res.Err != nil {
		t.Fatalf("reserve failed: %+v", res)
	}
	if ok, err := dst.executeCredit("bob", 300, res.TxnID); !ok || err != nil {
		t.Fatalf("credit failed: ok=%v err=%v", ok, err)
	}
	if ok, err := src.executeFinalize("tx_flow"); !ok || err != nil {
		t.Fatalf("finalize failed: ok=%v err=%v", ok, err)
	}

	if bal := src.Account["alice"].GetBalance(); bal != 700 {
		t.Fatalf("alice should be 700, got %d", bal)
	}
	if bal := dst.Account["bob"].GetBalance(); bal != 800 {
		t.Fatalf("bob should be 800, got %d", bal)
	}
	// After finalize, the source record must read success.
	if rec := src.AttemptedTransactions["tx_flow"]; rec.Status != StatusSuccess {
		t.Fatalf("expected StatusSuccess after finalize, got %d", rec.Status)
	}
}

// Refund must make the source whole when credit can't land, so no money vanishes.
func TestRefundRestoresFunds(t *testing.T) {
	src := InitialiseLedger()
	_ = src.executePureInitialise("alice", 1000)

	res := src.executeReserve("alice", "ghost", 300, "tx_refund")
	if !res.Proceed {
		t.Fatalf("reserve should have proceeded: %+v", res)
	}
	if bal := src.Account["alice"].GetBalance(); bal != 700 {
		t.Fatalf("alice should be held down to 700, got %d", bal)
	}

	// Credit would fail (ghost doesn't exist), so coordinator refunds.
	if ok, err := src.executeRefund("tx_refund"); !ok || err != nil {
		t.Fatalf("refund failed: ok=%v err=%v", ok, err)
	}
	if bal := src.Account["alice"].GetBalance(); bal != 1000 {
		t.Fatalf("alice should be made whole at 1000, got %d", bal)
	}
	if rec := src.AttemptedTransactions["tx_refund"]; rec.Status != StatusFailedFunds {
		t.Fatalf("expected StatusFailedFunds after refund, got %d", rec.Status)
	}
}

// Reserve's three reply signals.
func TestReserveReplySignals(t *testing.T) {
	ledger := InitialiseLedger()
	_ = ledger.executePureInitialise("alice", 1000)

	// Fresh: Proceed.
	if res := ledger.executeReserve("alice", "bob", 100, "k1"); !res.Proceed {
		t.Fatalf("first reserve should Proceed, got %+v", res)
	}
	// Same key still pending -> "already being processed" error, NOT Proceed.
	if res := ledger.executeReserve("alice", "bob", 100, "k1"); res.Proceed || res.Err == nil {
		t.Fatalf("pending retry should error, got %+v", res)
	}
	// Once finalized, the same key reports AlreadyDone so the coordinator skips crediting.
	_, _ = ledger.executeFinalize("k1")
	if res := ledger.executeReserve("alice", "bob", 100, "k1"); !res.AlreadyDone || res.Proceed {
		t.Fatalf("finalized retry should be AlreadyDone and not Proceed, got %+v", res)
	}
	// Payload mismatch on a known key -> error.
	if res := ledger.executeReserve("alice", "bob", 999, "k1"); res.Err == nil {
		t.Fatalf("mismatched amount should error, got %+v", res)
	}
	// Insufficient funds -> error, source untouched.
	if res := ledger.executeReserve("alice", "bob", 10_000_000, "k2"); res.Err == nil {
		t.Fatalf("over-balance reserve should error, got %+v", res)
	}
	if bal := ledger.Account["alice"].GetBalance(); bal != 900 {
		t.Fatalf("alice should only reflect the one finalized -100, got %d", bal)
	}
}
