package main

import (
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// balanceMetadataSym is the ScSymbol key used for SAC/SEP-41 Balance entries.
// It matches the Stellar SDK's internal constant.
var balanceMetadataSym = xdr.ScSymbol("Balance")

// contractBalanceFromData decodes both native-XLM and issued-asset SAC balance
// entries. The Stellar SDK's sac.ContractBalanceFromContractData helper
// intentionally omits native lumens, so Bronze extraction and historical
// enrichment share this decoder instead.
func contractBalanceFromData(contractData xdr.ContractDataEntry) ([32]byte, *big.Int, bool) {
	if contractData.Durability != xdr.ContractDataDurabilityPersistent {
		return [32]byte{}, nil, false
	}
	if contractData.Contract.ContractId == nil {
		return [32]byte{}, nil, false
	}

	keyEnumVecPtr, ok := contractData.Key.GetVec()
	if !ok || keyEnumVecPtr == nil {
		return [32]byte{}, nil, false
	}
	keyEnumVec := *keyEnumVecPtr
	if len(keyEnumVec) != 2 || !keyEnumVec[0].Equals(xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &balanceMetadataSym,
	}) {
		return [32]byte{}, nil, false
	}

	scAddress, ok := keyEnumVec[1].GetAddress()
	if !ok {
		return [32]byte{}, nil, false
	}
	holder, ok := scAddress.GetContractId()
	if !ok {
		return [32]byte{}, nil, false
	}

	balanceMapPtr, ok := contractData.Val.GetMap()
	if !ok || balanceMapPtr == nil {
		return [32]byte{}, nil, false
	}
	balanceMap := *balanceMapPtr
	if len(balanceMap) != 3 {
		return [32]byte{}, nil, false
	}
	if keySym, ok := balanceMap[0].Key.GetSym(); !ok || keySym != "amount" {
		return [32]byte{}, nil, false
	}
	if keySym, ok := balanceMap[1].Key.GetSym(); !ok || keySym != "authorized" || !balanceMap[1].Val.IsBool() {
		return [32]byte{}, nil, false
	}
	if keySym, ok := balanceMap[2].Key.GetSym(); !ok || keySym != "clawback" || !balanceMap[2].Val.IsBool() {
		return [32]byte{}, nil, false
	}
	amount, ok := balanceMap[0].Val.GetI128()
	if !ok || int64(amount.Hi) < 0 {
		return [32]byte{}, nil, false
	}
	value := new(big.Int).Lsh(new(big.Int).SetInt64(int64(amount.Hi)), 64)
	value.Add(value, new(big.Int).SetUint64(uint64(amount.Lo)))
	return holder, value, true
}

func decodeContractBalanceDataXDR(encoded string) (holder string, balance string, ok bool, err error) {
	var contractData xdr.ContractDataEntry
	if err := xdr.SafeUnmarshalBase64(encoded, &contractData); err != nil {
		return "", "", false, fmt.Errorf("unmarshal ContractDataEntry: %w", err)
	}

	holderBytes, amount, ok := contractBalanceFromData(contractData)
	if !ok {
		return "", "", false, nil
	}
	holder, err = strkey.Encode(strkey.VersionByteContract, holderBytes[:])
	if err != nil {
		return "", "", false, fmt.Errorf("encode balance holder: %w", err)
	}
	return holder, amount.String(), true, nil
}
