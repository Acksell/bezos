// Package projectionexpressions provides parsing and application of DynamoDB ProjectionExpressions.
//
// A ProjectionExpression is a string that identifies the attributes that you want
// to retrieve from a table or index. You can use it to minimize the amount of data
// transferred from DynamoDB to your application.
//
// Example usage:
//
//	projected, err := projectionexpressions.Project(expr, names, doc)
//	if err != nil {
//	    return err
//	}
package projectionexpressions

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpressions/ast"
	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpressions/parser"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Parse parses a ProjectionExpression string into an AST.
func Parse(expr string) (*ast.ProjectionExpression, error) {
	return parser.ParseExpr(expr)
}

// ApplyInput contains the expression attribute names for resolving aliases.
type ApplyInput struct {
	ExpressionNames map[string]string
}

// Apply applies a parsed projection expression to a document, returning only the projected attributes.
// It extracts only the attributes specified in the projection expression from the source document.
func Apply(expr *ast.ProjectionExpression, input ApplyInput, doc map[string]types.AttributeValue) (result map[string]types.AttributeValue, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if doc == nil {
		return nil, nil
	}

	result = make(map[string]types.AttributeValue)

	for _, path := range expr.Paths {
		if err := projectPath(path, input.ExpressionNames, doc, result); err != nil {
			// In DynamoDB, missing attributes in projections are silently ignored
			// Only return errors for actual problems (like type mismatches)
			continue
		}
	}

	return result, nil
}

// Project parses and applies a projection expression to an item in one step.
// If projExpr is nil or empty, returns the original item unchanged.
// This is a convenience function for the common use case of applying a projection.
func Project(projExpr *string, exprNames map[string]string, item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	if projExpr == nil || *projExpr == "" {
		return item, nil
	}
	if item == nil {
		return nil, nil
	}

	parsed, err := Parse(*projExpr)
	if err != nil {
		return nil, fmt.Errorf("parse projection expression: %w", err)
	}

	return Apply(parsed, ApplyInput{ExpressionNames: exprNames}, item)
}

// ProjectAll parses and applies a projection expression to multiple items.
// If projExpr is nil or empty, returns the original items unchanged.
// This is more efficient than calling Project repeatedly as it only parses once.
func ProjectAll(projExpr *string, exprNames map[string]string, items []map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	if projExpr == nil || *projExpr == "" {
		return items, nil
	}

	parsed, err := Parse(*projExpr)
	if err != nil {
		return nil, fmt.Errorf("parse projection expression: %w", err)
	}

	input := ApplyInput{ExpressionNames: exprNames}
	result := make([]map[string]types.AttributeValue, len(items))
	for i, item := range items {
		projected, err := Apply(parsed, input, item)
		if err != nil {
			return nil, err
		}
		result[i] = projected
	}
	return result, nil
}

// projectPath extracts a single path from the source document and sets it in the result.
func projectPath(path *ast.AttributePath, names map[string]string, source, result map[string]types.AttributeValue) error {
	if len(path.Parts) == 0 {
		return fmt.Errorf("empty path")
	}

	// Get the value at the path from source
	value, err := getPathValue(path, names, source)
	if err != nil {
		return err // Path doesn't exist or type error
	}

	// Set the value at the same path in result, creating intermediate structures as needed
	return setPathValue(path, names, result, value)
}

// getPathValue retrieves the value at the given path from the document.
func getPathValue(path *ast.AttributePath, names map[string]string, doc map[string]types.AttributeValue) (types.AttributeValue, error) {
	if len(path.Parts) == 0 {
		return nil, fmt.Errorf("empty path")
	}

	current := types.AttributeValue(&types.AttributeValueMemberM{Value: doc})

	for i, part := range path.Parts {
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			m, ok := current.(*types.AttributeValueMemberM)
			if !ok {
				return nil, fmt.Errorf("cannot access attribute %q on non-map type", name)
			}
			val, ok := m.Value[name]
			if !ok {
				return nil, fmt.Errorf("attribute %q not found", name)
			}
			current = val
		} else if part.Index != nil {
			l, ok := current.(*types.AttributeValueMemberL)
			if !ok {
				return nil, fmt.Errorf("cannot index non-list type at part %d", i)
			}
			idx := *part.Index
			if idx < 0 || idx >= len(l.Value) {
				return nil, fmt.Errorf("index %d out of bounds", idx)
			}
			current = l.Value[idx]
		}
	}

	return current, nil
}

// setPathValue sets the value at the given path in the result document,
// recreating the full structure including intermediate maps and lists.
// For list indices, DynamoDB returns a list containing just that element.
// E.g., "items[0]" returns {"items": {"L": [<value>]}}
func setPathValue(path *ast.AttributePath, names map[string]string, result map[string]types.AttributeValue, value types.AttributeValue) error {
	if len(path.Parts) == 0 {
		return fmt.Errorf("empty path")
	}

	// Simple single-part path
	if len(path.Parts) == 1 {
		part := path.Parts[0]
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			result[name] = value
			return nil
		}
		return fmt.Errorf("path must start with an identifier")
	}

	// Build the nested structure from the inside out
	// Start with the value and wrap it according to the path parts (in reverse)
	current := value
	for i := len(path.Parts) - 1; i >= 1; i-- {
		part := path.Parts[i]
		if part.Index != nil {
			// Wrap in a list containing just this element
			current = &types.AttributeValueMemberL{Value: []types.AttributeValue{current}}
		} else if part.Identifier != nil {
			// Wrap in a map with this key
			name := part.Identifier.GetName(names)
			current = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{name: current}}
		}
	}

	// Now we have the wrapped value, need to merge it into result at the top-level key
	firstPart := path.Parts[0]
	if firstPart.Identifier == nil {
		return fmt.Errorf("path must start with an identifier")
	}
	topLevelName := firstPart.Identifier.GetName(names)

	// Merge the current value into the result
	existing, exists := result[topLevelName]
	if !exists {
		result[topLevelName] = current
		return nil
	}

	// Need to merge existing and current structures
	merged, err := mergeAttributeValues(existing, current)
	if err != nil {
		return err
	}
	result[topLevelName] = merged
	return nil
}

// mergeAttributeValues merges two attribute values, combining maps recursively.
// This is needed when multiple projection paths share a common prefix.
func mergeAttributeValues(existing, new types.AttributeValue) (types.AttributeValue, error) {
	existingMap, existingIsMap := existing.(*types.AttributeValueMemberM)
	newMap, newIsMap := new.(*types.AttributeValueMemberM)

	if existingIsMap && newIsMap {
		// Merge the two maps
		for k, v := range newMap.Value {
			if existingVal, ok := existingMap.Value[k]; ok {
				merged, err := mergeAttributeValues(existingVal, v)
				if err != nil {
					return nil, err
				}
				existingMap.Value[k] = merged
			} else {
				existingMap.Value[k] = v
			}
		}
		return existingMap, nil
	}

	// For non-maps (or mixed types), the new value wins
	return new, nil
}
