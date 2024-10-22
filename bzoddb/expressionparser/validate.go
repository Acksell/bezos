package expressionparser

import (
	"bezos/bzoddb/expressionparser/ast"
	"bezos/bzoddb/expressionparser/parser"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Condition struct {
	Condition        string
	ExpressionNames  map[string]string
	ExpressionValues map[string]types.AttributeValue
}

func ValidateCondition(c Condition, doc map[string]types.AttributeValue) (match bool, err error) {
	// errs := make(errorsMap)
	// p := newParser("", []byte(in), GlobalStore("errors", errs))
	// parsed, err := p.parse(g)
	// if err != nil {
	// 	return false, err
	// }
	defer func() {
		if r := recover(); r != nil {
			// error message is stored in the panic value, because AST uses panics atm
			// even in Eval() method. Modifying err value here will return it to the caller.
			err = fmt.Errorf("%v", r)
		}
	}()

	parsed, err := parser.Parse("validateCondition", []byte(c.Condition))
	if err != nil {
		return false, err
	}
	cond, ok := parsed.(ast.Condition)
	if !ok {
		return false, fmt.Errorf("expected ast.Condition, got %T", parsed)
	}
	v := cond.Eval(ast.Input{
		Document:         convertToASTVals(doc),
		ExpressionNames:  c.ExpressionNames,
		ExpressionValues: convertToASTVals(c.ExpressionValues),
	})
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
