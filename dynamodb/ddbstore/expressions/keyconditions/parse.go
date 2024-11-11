package keyconditions

import (
	"bezos/dynamodb/ddbstore/expressions/keyconditions/ast"
	"bezos/dynamodb/table"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// External API for the parser
type KeyConditionParams struct {
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue // should we really tie ourselves to aws sdk v2?
	TableKeys                 table.PrimaryKeyDefinition      // should we really use the table package here?
}

// Internal API for the parser
type keyConditionParserParams struct {
	ExpressionKeyNames  map[string]string
	ExpressionKeyValues map[string]ast.KeyValue
	TableKeys           table.PrimaryKeyDefinition // can use internal type instead
}

const (
	globalStoreParamsKey = "keyCondParams"
)

func ParseKeyCondition(expr string, keyParams KeyConditionParams) (*ast.KeyCondition, error) {
	parserParams := toParserParams(keyParams)
	// todo put in internal package?
	v, err := Parse("keyConditionParser", []byte(expr), GlobalStore(globalStoreParamsKey, parserParams))
	if err != nil {
		return nil, err
	}
	ast, ok := v.(*ast.KeyCondition)
	if !ok {
		return nil, fmt.Errorf("expected *ast.KeyCondition, got %T", v)
	}
	return ast, nil
}

func toParserParams(params KeyConditionParams) *keyConditionParserParams {
	return &keyConditionParserParams{
		ExpressionKeyNames:  params.ExpressionAttributeNames,
		ExpressionKeyValues: toKeyValues(params.ExpressionAttributeValues),
		TableKeys:           params.TableKeys,
	}
}

func toKeyValues(attrs map[string]types.AttributeValue) map[string]ast.KeyValue {
	res := make(map[string]ast.KeyValue)
	for k, v := range attrs {
		res[k] = toKeyValue(v)
	}
	return res
}

func toKeyValue(attr types.AttributeValue) ast.KeyValue {
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		return ast.KeyValue{Value: v.Value, Type: ast.STRING}
	case *types.AttributeValueMemberN:
		return ast.KeyValue{Value: v.Value, Type: ast.NUMBER}
	case *types.AttributeValueMemberB:
		return ast.KeyValue{Value: v.Value, Type: ast.BINARY}
	default:
		panic(fmt.Errorf("unsupported attribute value type %T", attr))
	}
}
