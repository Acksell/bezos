package keyconditionexpr

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/keyconditionexpr/ast"
	"github.com/acksell/bezos/dynamodb/ddbstore/keyconditionexpr/parser"
	"github.com/acksell/bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// External API for the parser.
type ParseParams struct {
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	TableKeys                 table.PrimaryKeyDefinition
}

func Parse(expr string, params ParseParams) (*ast.KeyCondition, error) {
	parserParams := toParserParams(params)
	return parser.ParseExpr(expr, *parserParams)
}

func toParserParams(params ParseParams) *parser.KeyConditionParserParams {
	return &parser.KeyConditionParserParams{
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
