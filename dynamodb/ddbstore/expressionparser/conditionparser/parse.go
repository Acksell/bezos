package conditionparser

import (
	"bezos/dynamodb/ddbstore/expressionparser/conditionast"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ConditionInput struct {
	Condition        string
	ExpressionNames  map[string]string
	ExpressionValues map[string]types.AttributeValue
}

func ParseCondition(condition string) (cond conditionast.Condition, err error) {
	defer func() {
		if r := recover(); r != nil {
			// error message is stored in the panic value, because AST uses panics atm
			// even in Eval() method. Modifying err value here will return it to the caller.
			err = fmt.Errorf("%v", r)
		}
	}()
	// todo put in internal package?
	parsed, err := Parse("parseCondition", []byte(condition))
	if err != nil {
		return nil, err
	}
	cond, ok := parsed.(conditionast.Condition)
	if !ok {
		return nil, fmt.Errorf("expected ast.Condition, got %T", parsed)
	}
	return cond, nil
}

func EvalCondition(c ConditionInput, doc map[string]types.AttributeValue) (match bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// error message is stored in the panic value, because AST uses panics atm
			// even in Eval() method. Modifying err value here will return it to the caller.
			err = fmt.Errorf("%v", r)
		}
	}()
	cond, err := ParseCondition(c.Condition)
	if err != nil {
		return false, err
	}
	v := cond.Eval(conditionast.Input{
		ExpressionNames:  c.ExpressionNames,
		ExpressionValues: convertToASTVals(c.ExpressionValues),
	}, convertToASTVals(doc))
	return v, nil
}

// AST package uses internal types in order to decouple it from AWS DDB SDK versions.
func convertToASTVals(vals map[string]types.AttributeValue) map[string]conditionast.AttributeValue {
	astMap := make(map[string]conditionast.AttributeValue)
	for k, v := range vals {
		astMap[k] = convertToASTVal(v)
	}
	return astMap
}

func convertToASTVal(val types.AttributeValue) conditionast.AttributeValue {
	switch v := val.(type) {
	case *types.AttributeValueMemberM:
		return conditionast.AttributeValue{
			Value: convertToASTVals(v.Value),
			Type:  conditionast.MAP,
		}
	case *types.AttributeValueMemberL:
		values := make([]conditionast.AttributeValue, 0, len(v.Value))
		for _, val := range v.Value {
			values = append(values, convertToASTVal(val))
		}
		return conditionast.AttributeValue{
			Value: values,
			Type:  conditionast.LIST,
		}
	case *types.AttributeValueMemberS:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.STRING,
		}
	case *types.AttributeValueMemberN:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.NUMBER,
		}
	case *types.AttributeValueMemberB:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.BINARY,
		}
	case *types.AttributeValueMemberBOOL:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.BOOL,
		}
	case *types.AttributeValueMemberNULL:
		return conditionast.AttributeValue{
			Value: nil,
			Type:  conditionast.NULL,
		}
	case *types.AttributeValueMemberSS:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.STRING_SET,
		}
	case *types.AttributeValueMemberNS:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.NUMBER_SET,
		}
	case *types.AttributeValueMemberBS:
		return conditionast.AttributeValue{
			Value: v.Value,
			Type:  conditionast.BINARY_SET,
		}
	default:
		panic(fmt.Sprintf("unsupported attribute type %T", v))
	}
}
