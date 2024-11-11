package parser

import (
	"bezos/dynamodb/ddbstore/expressions/writeconditions/ast"
	"fmt"
)

func ParseExpr(condition string) (cond ast.Condition, err error) {
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
	cond, ok := parsed.(ast.Condition)
	if !ok {
		return nil, fmt.Errorf("expected ast.Condition, got %T", parsed)
	}
	return cond, nil
}
