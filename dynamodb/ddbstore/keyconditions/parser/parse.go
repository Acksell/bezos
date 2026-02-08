package parser

import (
	"bezos/dynamodb/ddbstore/keyconditions/ast"
	"bezos/dynamodb/table"
	"fmt"
)

// Internal API for the parser
type KeyConditionParserParams struct {
	ExpressionKeyNames  map[string]string
	ExpressionKeyValues map[string]ast.KeyValue
	TableKeys           table.PrimaryKeyDefinition // can use internal type instead
}

const (
	globalStoreParamsKey = "keyCondParams"
)

// Users should use ParseExpr, not Parse directly.
// Because Parse is already taken.
func ParseExpr(expr string, params KeyConditionParserParams) (_ *ast.KeyCondition, err error) {
	defer func() {
		if r := recover(); r != nil {
			// error message is stored in the panic value, because AST uses panics atm
			// even in Eval() method. Modifying err value here will return it to the caller.
			err = fmt.Errorf("%v", r)
		}
	}()
	v, err := Parse("keyConditionParser", []byte(expr), GlobalStore(globalStoreParamsKey, &params))
	if err != nil {
		return nil, err
	}
	ast, ok := v.(*ast.KeyCondition)
	if !ok {
		return nil, fmt.Errorf("expected *ast.KeyCondition, got %T", v)
	}
	return ast, nil
}
