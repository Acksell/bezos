package ddbcli

import (
	"encoding/base64"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ItemToJSON converts a DynamoDB item to a JSON-friendly map.
func ItemToJSON(item map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any, len(item))
	for k, v := range item {
		result[k] = attributeValueToJSON(v)
	}
	return result
}

// ItemsToJSON converts multiple DynamoDB items to JSON-friendly format.
func ItemsToJSON(items []map[string]types.AttributeValue) []map[string]any {
	result := make([]map[string]any, len(items))
	for i, item := range items {
		result[i] = ItemToJSON(item)
	}
	return result
}

func attributeValueToJSON(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return f
		}
		return v.Value
	case *types.AttributeValueMemberB:
		return base64.StdEncoding.EncodeToString(v.Value)
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, elem := range v.Value {
			list[i] = attributeValueToJSON(elem)
		}
		return list
	case *types.AttributeValueMemberM:
		m := make(map[string]any)
		for k, elem := range v.Value {
			m[k] = attributeValueToJSON(elem)
		}
		return m
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		list := make([]string, len(v.Value))
		for i, b := range v.Value {
			list[i] = base64.StdEncoding.EncodeToString(b)
		}
		return list
	default:
		return nil
	}
}
