package writeconditions

import (
	"bezos/dynamodb/ddbstore/expressions/writeconditions/ast"
	"bezos/dynamodb/ddbstore/expressions/writeconditions/parser"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func Parse(condition string) (ast.Condition, error) {
	return parser.ParseExpr(condition)
}

type EvalInput struct {
	ExpressionNames  map[string]string
	ExpressionValues map[string]types.AttributeValue
}

// todo use errors instead of panics for error messages?
// todo expose a validate function to ast that parser can call? and thus avoid panics?
func Eval(condition string, input EvalInput, doc map[string]types.AttributeValue) (match bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// error message is stored in the panic value, because AST uses panics atm
			// even in Eval() method. Modifying err value here will return it to the caller.
			err = fmt.Errorf("%v", r)
		}
	}()
	cond, err := Parse(condition)
	if err != nil {
		return false, err
	}
	v := cond.Eval(ast.Input{
		ExpressionNames:  input.ExpressionNames,
		ExpressionValues: convertToASTVals(input.ExpressionValues),
	}, convertToASTVals(doc))
	return v, nil
}

// AST package uses internal types in order to decouple it from AWS DDB SDK versions.
func convertToASTVals(vals map[string]types.AttributeValue) map[string]ast.AttributeValue {
	astMap := make(map[string]ast.AttributeValue)
	for k, v := range vals {
		astMap[k] = convertToASTVal(v)
	}
	return astMap
}

func convertToASTVal(val types.AttributeValue) ast.AttributeValue {
	switch v := val.(type) {
	case *types.AttributeValueMemberM:
		return ast.AttributeValue{
			Value: convertToASTVals(v.Value),
			Type:  ast.MAP,
		}
	case *types.AttributeValueMemberL:
		values := make([]ast.AttributeValue, 0, len(v.Value))
		for _, val := range v.Value {
			values = append(values, convertToASTVal(val))
		}
		return ast.AttributeValue{
			Value: values,
			Type:  ast.LIST,
		}
	case *types.AttributeValueMemberS:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.STRING,
		}
	case *types.AttributeValueMemberN:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.NUMBER,
		}
	case *types.AttributeValueMemberB:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.BINARY,
		}
	case *types.AttributeValueMemberBOOL:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.BOOL,
		}
	case *types.AttributeValueMemberNULL:
		return ast.AttributeValue{
			Value: nil,
			Type:  ast.NULL,
		}
	case *types.AttributeValueMemberSS:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.STRING_SET,
		}
	case *types.AttributeValueMemberNS:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.NUMBER_SET,
		}
	case *types.AttributeValueMemberBS:
		return ast.AttributeValue{
			Value: v.Value,
			Type:  ast.BINARY_SET,
		}
	default:
		panic(fmt.Sprintf("unsupported attribute type %T", v))
	}
}
