package parser

import (
	"bezos/dynamodb/ddbstore/updateexpressions/ast"
	"fmt"
)

// ParseExpr parses an UpdateExpression string into an AST.
func ParseExpr(expr string) (result *ast.UpdateExpression, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// Initialize the section tracker to enforce section uniqueness
	globalStore := map[string]any{
		"sectionTracker": &sectionTracker{},
	}

	parsed, err := Parse("updateExpression", []byte(expr), GlobalStore("sectionTracker", globalStore["sectionTracker"]))
	if err != nil {
		return nil, err
	}

	result, ok := parsed.(*ast.UpdateExpression)
	if !ok {
		return nil, fmt.Errorf("expected *ast.UpdateExpression, got %T", parsed)
	}

	return result, nil
}
