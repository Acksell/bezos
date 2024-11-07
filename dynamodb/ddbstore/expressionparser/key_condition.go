package expressionparser

import (
	"bezos/dynamodb/ddbstore/expressionparser/ast"
	"fmt"
)

type KeyCondition struct {
	PartitionKeyValue any
	SortKeyCondition  ast.Condition
}

// feels like this should be done in the ast package instead. One method to EvalFilter() and another to EvalKeyCondition()
func AsKeyCondition(cond ast.Condition, input ast.Input) (*KeyCondition, error) {
	keyCond := &KeyCondition{}
	pkname := input.KeyNames.PartitionKeyName
	switch v := cond.(type) {
	// Just a single comparison, must be for the PK
	case *ast.Comparison:
		pkVal, err := pkFromComparison(pkname, v, input)
		if err != nil {
			return nil, fmt.Errorf("extract pk from comparison: %w", err)
		}
		keyCond.PartitionKeyValue = pkVal
	// Both PK and SK condition included, but not sure which one is left or right.
	case *ast.LogicalOp:
		if v.Operator != ast.AND {
			return nil, fmt.Errorf("unsupported logical operator %q for key condition", v.Operator)
		}
		keyNameLeft, err := getKeyName(v.Left, input)
		if err != nil {
			return nil, fmt.Errorf("get key name from condition: %w", err)
		}
		keyNameRight, err := getKeyName(v.Right, input)
		if err != nil {
			return nil, fmt.Errorf("get key name from condition: %w", err)
		}
		switch input.KeyNames.PartitionKeyName {
		case keyNameLeft:
			pkVal, err := pkFromComparison(keyNameLeft, v.Left, input)
			if err != nil {
				return nil, fmt.Errorf("extract pk from comparison: %w", err)
			}
			keyCond.PartitionKeyValue = pkVal

			err = validateSK(input.KeyNames.SortKeyName, v.Right, input)
			if err != nil {
				return nil, fmt.Errorf("validate sort key: %w", err)
			}
			keyCond.SortKeyCondition = v.Right
		case keyNameRight:
			pkVal, err := pkFromComparison(keyNameRight, v.Right, input)
			if err != nil {
				return nil, fmt.Errorf("extract pk from comparison: %w", err)
			}
			keyCond.PartitionKeyValue = pkVal

			err = validateSK(input.KeyNames.SortKeyName, v.Left, input)
			if err != nil {
				return nil, fmt.Errorf("validate sort key: %w", err)
			}
			keyCond.SortKeyCondition = v.Left
		default:
			return nil, fmt.Errorf("partition key condition %q not found in key condition", pkname)
		}
	default:
		return nil, fmt.Errorf("unsupported condition %T for key condition", cond)
	}
	return keyCond, nil
}

func getKeyName(cond ast.Condition, input ast.Input) (string, error) {
	switch v := cond.(type) {
	case *ast.Comparison:
		return getKeyNameFromExpression(v.Left, input) // todo are key names always on the left?
	case *ast.BetweenExpr:
		return getKeyNameFromExpression(v.Val, input)
	case *ast.FunctionCall:
		if v.FunctionName != "begins_with" {
			return "", fmt.Errorf("unsupported function %q for key condition", v.FunctionName)
		}
		return getKeyNameFromExpression(v.Args[0], input)
	}
	return "", fmt.Errorf("unsupported condition term %T for key condition", cond)
}

func getKeyNameFromExpression(expr ast.Expression, input ast.Input) (string, error) {
	switch e := expr.(type) {
	case *ast.AttributePath:
		path := e
		if len(path.Parts) != 1 {
			return "", fmt.Errorf("attributes in key conditions cannot be nested")
		}
		return path.Parts[0].Identifier.GetName(input), nil
	default:
		return "", fmt.Errorf("expression must be a valid identifier: %v", e)
	}
}

func pkFromComparison(keyName string, cond ast.Condition, input ast.Input) (any, error) {
	cmp, ok := cond.(*ast.Comparison)
	if !ok {
		return nil, fmt.Errorf("unsupported condition %T for partition key in key condition", cond)
	}
	err := verifyIdentifierMatchesKey(keyName, cmp.Left, input)
	if err != nil {
		return nil, fmt.Errorf("verify identifier matches key: %w", err)
	}
	switch cmp.Operator {
	case ast.Equal:
		return cmp.Right.GetValue(input).Value, nil
	default:
		return nil, fmt.Errorf("unsupported operator %q for partition key", cmp.Operator)
	}
}

func validateSK(keyName string, rangeCond ast.Condition, input ast.Input) error {
	switch v := rangeCond.(type) {
	case *ast.Comparison:
		switch v.Operator {
		case ast.Equal, ast.GreaterThan, ast.GreaterOrEqual, ast.LessThan, ast.LessOrEqual:
		case ast.NotEqual:
			fallthrough
		default:
			return fmt.Errorf("unsupported operator %q for sort key condition", v.Operator)
		}
		err := verifyIdentifierMatchesKey(keyName, v.Left, input)
		if err != nil {
			return fmt.Errorf("verify identifier matches key: %w", err)
		}
	case *ast.BetweenExpr:
		err := verifyIdentifierMatchesKey(keyName, v.Val, input)
		if err != nil {
			return fmt.Errorf("verify identifier matches key: %w", err)
		}
	case *ast.FunctionCall:
		if v.FunctionName != "begins_with" {
			return fmt.Errorf("unsupported function %q for sort key condition", v.FunctionName)
		}
		err := verifyIdentifierMatchesKey(keyName, v.Args[0], input)
		if err != nil {
			return fmt.Errorf("verify identifier matches key: %w", err)
		}
	default:
		return fmt.Errorf("unsupported range condition %T for sort key", rangeCond)
	}
	return nil
}

func verifyIdentifierMatchesKey(keyName string, expr ast.Expression, input ast.Input) error {
	name, err := getKeyNameFromExpression(expr, input)
	if err != nil {
		return fmt.Errorf("get key name from expression: %w", err)
	}
	fmt.Println("keyName:", keyName, "name:", name)
	if keyName != name {
		return fmt.Errorf("expected key %q, got %q", keyName, name)
	}
	return nil
}
