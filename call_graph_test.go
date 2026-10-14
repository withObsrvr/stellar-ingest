package main

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractCallsFromAuthInvocationSkipsNonContractAddress(t *testing.T) {
	invocation := &xdr.SorobanAuthorizedInvocation{
		Function: xdr.SorobanAuthorizedFunction{
			Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
			ContractFn: &xdr.InvokeContractArgs{
				// This shape reproduces the crash class seen in ledger 59217475:
				// ContractFn exists, but ContractAddress.ContractId is nil because the
				// SCAddress arm is not a contract address.
				ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount},
				FunctionName:    xdr.ScSymbol("transfer"),
			},
		},
	}

	executionOrder := 0
	calls := extractCallsFromAuthInvocation(invocation, "CMAIN", 1, &executionOrder)
	if len(calls) != 0 {
		t.Fatalf("expected non-contract auth invocation to be skipped, got %d calls", len(calls))
	}
	if executionOrder != 0 {
		t.Fatalf("expected execution order to remain 0, got %d", executionOrder)
	}
}

func TestScAddressContractStrKeyCountsSkips(t *testing.T) {
	// Non-contract arm: skipped and counted, so the silent drop is observable.
	before := CallGraphSkippedEdges()
	if _, ok := scAddressContractStrKey(xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount}); ok {
		t.Fatal("expected non-contract address to return ok=false")
	}
	if got := CallGraphSkippedEdges(); got != before+1 {
		t.Fatalf("expected skip counter to increment by 1, got %d (was %d)", got, before)
	}

	// Valid contract arm: encodes successfully and does not increment the counter.
	var contractID xdr.ContractId
	contractID[0] = 1
	before = CallGraphSkippedEdges()
	if _, ok := scAddressContractStrKey(xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID}); !ok {
		t.Fatal("expected valid contract address to return ok=true")
	}
	if got := CallGraphSkippedEdges(); got != before {
		t.Fatalf("expected skip counter unchanged for valid address, got %d (was %d)", got, before)
	}
}

func TestExtractCallsFromAuthInvocationContractAddress(t *testing.T) {
	var contractID xdr.ContractId
	contractID[0] = 1
	toContract, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		t.Fatalf("encode contract: %v", err)
	}

	invocation := &xdr.SorobanAuthorizedInvocation{
		Function: xdr.SorobanAuthorizedFunction{
			Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
			ContractFn: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol("swap"),
			},
		},
	}

	executionOrder := 0
	calls := extractCallsFromAuthInvocation(invocation, "CMAIN", 1, &executionOrder)
	if len(calls) != 1 {
		t.Fatalf("expected one contract call, got %d", len(calls))
	}
	if calls[0].ToContract != toContract {
		t.Fatalf("expected to contract %s, got %s", toContract, calls[0].ToContract)
	}
	if calls[0].FunctionName != "swap" {
		t.Fatalf("expected function swap, got %s", calls[0].FunctionName)
	}
	if executionOrder != 1 {
		t.Fatalf("expected execution order 1, got %d", executionOrder)
	}
}
