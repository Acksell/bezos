package parser

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr/ast"
)

// ParseExpr parses a ProjectionExpression string into an AST.
func ParseExpr(expr string) (result *ast.ProjectionExpression, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	parsed, err := Parse("projectionExpression", []byte(expr))
	if err != nil {
		return nil, err
	}

	result, ok := parsed.(*ast.ProjectionExpression)
	if !ok {
		return nil, fmt.Errorf("expected *ast.ProjectionExpression, got %T", parsed)
	}

	return result, nil
}
