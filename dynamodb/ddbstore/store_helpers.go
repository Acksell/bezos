package ddbstore

import (
	"bytes"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func ptrStr(s string) *string {
	return &s
}

func attributeValuesEqual(a, b types.AttributeValue) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		if bv, ok := b.(*types.AttributeValueMemberS); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberN:
		if bv, ok := b.(*types.AttributeValueMemberN); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberB:
		if bv, ok := b.(*types.AttributeValueMemberB); ok {
			return bytes.Equal(av.Value, bv.Value)
		}
	}
	return false
}

func extractKeyAttributes(item map[string]types.AttributeValue, keyDef table.PrimaryKeyDefinition) map[string]types.AttributeValue {
	result := make(map[string]types.AttributeValue)
	if pk, ok := item[keyDef.PartitionKey.Name]; ok {
		result[keyDef.PartitionKey.Name] = pk
	}
	if keyDef.SortKey.Name != "" {
		if sk, ok := item[keyDef.SortKey.Name]; ok {
			result[keyDef.SortKey.Name] = sk
		}
	}
	return result
}

func incrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)
	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result
		}
		result[i] = 0
	}
	// Overflow - append 0x00
	return append(result, 0x00)
}
