package main

import (
	"log"
	"sync/atomic"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// extractCallsFromAuthInvocation recursively extracts calls from auth invocations
func extractCallsFromAuthInvocation(
	invocation *xdr.SorobanAuthorizedInvocation,
	fromContract string,
	depth int,
	executionOrder *int,
) []ContractCall {
	var calls []ContractCall

	if invocation == nil || depth > 10 { // Max depth protection
		return calls
	}

	// First, capture this invocation's function call (the root or current node).
	// This was previously missing - we only captured sub-invocations.
	// Some historical auth payloads carry a CONTRACT_FN arm whose ContractAddress
	// is not a contract ScAddress (e.g. an account/muxed arm). The generated union
	// getter returns ok=false for non-contract arms, so we skip those cleanly
	// instead of dereferencing ContractAddress.ContractId directly and panicking
	// the whole historical backfill.
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
		contractFn := invocation.Function.ContractFn
		if contractFn != nil {
			toContractID, ok := scAddressContractStrKey(contractFn.ContractAddress)
			if ok && toContractID != fromContract {
				// Only record if it's a cross-contract call (different from/to)
				functionName := string(contractFn.FunctionName)
				call := ContractCall{
					FromContract:   fromContract,
					ToContract:     toContractID,
					FunctionName:   functionName,
					CallDepth:      depth,
					ExecutionOrder: *executionOrder,
					Successful:     true,
				}
				calls = append(calls, call)
				*executionOrder++

				// Update fromContract for sub-invocations
				fromContract = toContractID
			}
		}
	}

	// Process sub-invocations recursively
	// The recursive call will capture each sub-invocation's function via the code above
	for _, subInvocation := range invocation.SubInvocations {
		subCalls := extractCallsFromAuthInvocation(
			&subInvocation,
			fromContract, // Pass current contract as the caller
			depth+1,
			executionOrder,
		)
		calls = append(calls, subCalls...)
	}

	return calls
}

func scAddressContractStrKey(address xdr.ScAddress) (string, bool) {
	contractID, ok := address.GetContractId()
	if !ok {
		// Non-contract arm (e.g. account/muxed address). Counted but not logged
		// per-occurrence; the aggregate is surfaced at end of run so silent
		// call-graph under-counting stays observable without log spam.
		callGraphSkippedEdges.Add(1)
		return "", false
	}
	encoded, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		// A contract-arm address that fails strkey encoding is genuinely
		// unexpected; count it and log per-occurrence.
		callGraphSkippedEdges.Add(1)
		log.Printf("[call_graph] failed to strkey-encode contract address: %v", err)
		return "", false
	}
	return encoded, true
}

// callGraphSkippedEdges counts CONTRACT_FN auth entries skipped because their
// address was not an encodable contract address.
var callGraphSkippedEdges atomic.Int64

// CallGraphSkippedEdges returns the running skip count so callers can surface it
// at end of run and detect silent call-graph under-counting.
func CallGraphSkippedEdges() int64 { return callGraphSkippedEdges.Load() }

// findFnNameRecursive recursively searches a JSON-like structure for fn_name
func findFnNameRecursive(v interface{}) string {
	switch val := v.(type) {
	case map[string]interface{}:
		// Check if this map has fn_name
		if fnName, ok := val["fn_name"]; ok {
			if str, ok := fnName.(string); ok && str != "" {
				return str
			}
		}
		// Recursively search all values
		for _, v := range val {
			if result := findFnNameRecursive(v); result != "" {
				return result
			}
		}
	case []interface{}:
		// Search through array elements
		for _, item := range val {
			if result := findFnNameRecursive(item); result != "" {
				return result
			}
		}
	}
	return ""
}
