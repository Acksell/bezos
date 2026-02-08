// Package updateexpressions provides parsing and evaluation of DynamoDB UpdateExpressions.
package updateexpressions

import (
	"bezos/dynamodb/ddbstore/updateexpressions/ast"
	"bezos/dynamodb/ddbstore/updateexpressions/parser"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Parse parses an UpdateExpression string into an AST.
func Parse(expr string) (*ast.UpdateExpression, error) {
	return parser.ParseExpr(expr)
}

// EvalInput contains the expression attribute names and values.
type EvalInput struct {
	ExpressionNames  map[string]string
	ExpressionValues map[string]types.AttributeValue
	ReturnValues     types.ReturnValue
}

// EvalOutput contains the result of applying an update expression.
// Note: Note thread safe.
type EvalOutput struct {
	// Item is the document after the update expression has been applied.
	Item map[string]types.AttributeValue
	// ReturnAttributes contains the attributes to return based on ReturnValues setting.
	// For NONE: nil
	// For ALL_OLD: the entire old item
	// For ALL_NEW: the entire new item
	// For UPDATED_OLD: only the updated attributes from the old item
	// For UPDATED_NEW: only the updated attributes from the new item
	ReturnAttributes map[string]types.AttributeValue
}

// Apply applies the parsed UpdateExpression to a document, returning the modified document
// and any return attributes based on the ReturnValues setting.
// The oldItem parameter is the item before any modifications (can be nil for new items).
func Apply(expr *ast.UpdateExpression, input EvalInput, oldItem map[string]types.AttributeValue) (*EvalOutput, error) {
	// Make a copy of the old item to work with
	doc := make(map[string]types.AttributeValue)
	for k, v := range oldItem {
		doc[k] = v
	}

	// Validate no cross-clause path overlaps
	if err := validateNoPathOverlap(expr, input.ExpressionNames); err != nil {
		return nil, err
	}

	// Process SET actions
	for _, action := range expr.SetActions {
		val, err := evaluateSetValue(action.Value, input, doc)
		if err != nil {
			return nil, fmt.Errorf("SET: %w", err)
		}
		if err := setPath(action.Path, input.ExpressionNames, doc, val); err != nil {
			return nil, fmt.Errorf("SET: %w", err)
		}
	}

	// Process REMOVE actions
	for _, action := range expr.RemoveActions {
		if err := removePath(action.Path, input.ExpressionNames, doc); err != nil {
			return nil, fmt.Errorf("REMOVE: %w", err)
		}
	}

	// Process ADD actions
	for _, action := range expr.AddActions {
		val, err := evaluateOperand(action.Value, input, doc)
		if err != nil {
			return nil, fmt.Errorf("ADD: %w", err)
		}
		if err := addToPath(action.Path, input.ExpressionNames, doc, val); err != nil {
			return nil, fmt.Errorf("ADD: %w", err)
		}
	}

	// Process DELETE actions
	for _, action := range expr.DeleteActions {
		val, err := evaluateOperand(action.Value, input, doc)
		if err != nil {
			return nil, fmt.Errorf("DELETE: %w", err)
		}
		if err := deleteFromPath(action.Path, input.ExpressionNames, doc, val); err != nil {
			return nil, fmt.Errorf("DELETE: %w", err)
		}
	}

	// Compute return attributes based on ReturnValues setting
	var returnAttrs map[string]types.AttributeValue
	switch input.ReturnValues {
	case types.ReturnValueAllOld:
		returnAttrs = oldItem
	case types.ReturnValueAllNew:
		returnAttrs = doc
	case types.ReturnValueUpdatedOld:
		returnAttrs = extractUpdatedAttributes(expr, input.ExpressionNames, oldItem)
	case types.ReturnValueUpdatedNew:
		returnAttrs = extractUpdatedAttributes(expr, input.ExpressionNames, doc)
	}

	return &EvalOutput{
		Item:             doc,
		ReturnAttributes: returnAttrs,
	}, nil
}

// extractUpdatedAttributes returns only the top-level attributes that were modified.
// DynamoDB returns the entire top-level attribute, not just the nested path that changed.
func extractUpdatedAttributes(expr *ast.UpdateExpression, names map[string]string, item map[string]types.AttributeValue) map[string]types.AttributeValue {
	if item == nil {
		return nil
	}

	// Collect all top-level attribute names that were touched
	touched := make(map[string]struct{})

	for _, action := range expr.SetActions {
		if name := getTopLevelName(action.Path, names); name != "" {
			touched[name] = struct{}{}
		}
	}
	for _, action := range expr.RemoveActions {
		if name := getTopLevelName(action.Path, names); name != "" {
			touched[name] = struct{}{}
		}
	}
	for _, action := range expr.AddActions {
		if name := getTopLevelName(action.Path, names); name != "" {
			touched[name] = struct{}{}
		}
	}
	for _, action := range expr.DeleteActions {
		if name := getTopLevelName(action.Path, names); name != "" {
			touched[name] = struct{}{}
		}
	}

	// Extract values for touched attributes
	result := make(map[string]types.AttributeValue)
	for name := range touched {
		if val, ok := item[name]; ok {
			result[name] = val
		}
	}
	return result
}

// getTopLevelName returns the top-level attribute name from a path.
func getTopLevelName(path *ast.AttributePath, names map[string]string) string {
	if len(path.Parts) > 0 && path.Parts[0].Identifier != nil {
		return path.Parts[0].Identifier.GetName(names)
	}
	return ""
}

// Update parses and applies an update expression in one step.
// This is a convenience function for when you don't need to reuse the parsed expression.
func Update(updateExpr string, input EvalInput, oldItem map[string]types.AttributeValue) (*EvalOutput, error) {
	expr, err := Parse(updateExpr)
	if err != nil {
		return nil, fmt.Errorf("parse update expression: %w", err)
	}
	return Apply(expr, input, oldItem)
}

// UpdateFromParams is a convenience function that extracts parameters from UpdateItemInput.
func UpdateFromParams(params *dynamodb.UpdateItemInput, oldItem map[string]types.AttributeValue) (*EvalOutput, error) {
	if params.UpdateExpression == nil {
		return nil, fmt.Errorf("UpdateExpression is required")
	}
	return Update(*params.UpdateExpression, EvalInput{
		ExpressionNames:  params.ExpressionAttributeNames,
		ExpressionValues: params.ExpressionAttributeValues,
		ReturnValues:     params.ReturnValues,
	}, oldItem)
}

func evaluateSetValue(v ast.SetValue, input EvalInput, doc map[string]types.AttributeValue) (types.AttributeValue, error) {
	switch val := v.(type) {
	case *ast.ArithmeticOp:
		left, err := evaluateOperand(val.Left, input, doc)
		if err != nil {
			return nil, err
		}
		right, err := evaluateOperand(val.Right, input, doc)
		if err != nil {
			return nil, err
		}
		return applyArithmetic(val.Operator, left, right)

	case ast.Operand:
		return evaluateOperand(val, input, doc)

	default:
		return nil, fmt.Errorf("unsupported SetValue type: %T", v)
	}
}

func evaluateOperand(op ast.Operand, input EvalInput, doc map[string]types.AttributeValue) (types.AttributeValue, error) {
	switch o := op.(type) {
	case *ast.ExpressionAttributeValue:
		val, ok := input.ExpressionValues[o.Alias]
		if !ok {
			return nil, fmt.Errorf("expression attribute value %s not found", o.Alias)
		}
		return val, nil

	case *ast.AttributePath:
		return getPathValue(o, input.ExpressionNames, doc)

	case *ast.IfNotExists:
		val, err := getPathValue(o.Path, input.ExpressionNames, doc)
		if err != nil {
			// Path doesn't exist, return the default value
			return evaluateOperand(o.Value, input, doc)
		}
		return val, nil

	case *ast.ListAppend:
		list1, err := evaluateOperand(o.List1, input, doc)
		if err != nil {
			return nil, err
		}
		list2, err := evaluateOperand(o.List2, input, doc)
		if err != nil {
			return nil, err
		}
		return appendLists(list1, list2)

	default:
		return nil, fmt.Errorf("unsupported Operand type: %T", op)
	}
}

func applyArithmetic(op string, left, right types.AttributeValue) (types.AttributeValue, error) {
	// Check for list concatenation with "+"
	if op == "+" {
		if leftList, ok := left.(*types.AttributeValueMemberL); ok {
			if rightList, ok := right.(*types.AttributeValueMemberL); ok {
				result := make([]types.AttributeValue, 0, len(leftList.Value)+len(rightList.Value))
				result = append(result, leftList.Value...)
				result = append(result, rightList.Value...)
				return &types.AttributeValueMemberL{Value: result}, nil
			}
		}
	}

	// Numeric arithmetic
	leftNum, ok := left.(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("left operand must be a number for arithmetic, got %T", left)
	}
	rightNum, ok := right.(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("right operand must be a number for arithmetic, got %T", right)
	}

	leftVal, err := parseNumber(leftNum.Value)
	if err != nil {
		return nil, err
	}
	rightVal, err := parseNumber(rightNum.Value)
	if err != nil {
		return nil, err
	}

	var result float64
	switch op {
	case "+":
		result = leftVal + rightVal
	case "-":
		result = leftVal - rightVal
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator: %s", op)
	}

	return &types.AttributeValueMemberN{Value: formatNumber(result)}, nil
}

func appendLists(left, right types.AttributeValue) (types.AttributeValue, error) {
	leftList, ok := left.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("list_append: first argument must be a list, got %T", left)
	}
	rightList, ok := right.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("list_append: second argument must be a list, got %T", right)
	}

	result := make([]types.AttributeValue, 0, len(leftList.Value)+len(rightList.Value))
	result = append(result, leftList.Value...)
	result = append(result, rightList.Value...)
	return &types.AttributeValueMemberL{Value: result}, nil
}

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

func setPath(path *ast.AttributePath, names map[string]string, doc map[string]types.AttributeValue, value types.AttributeValue) error {
	if len(path.Parts) == 0 {
		return fmt.Errorf("empty path")
	}

	if len(path.Parts) == 1 {
		part := path.Parts[0]
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			doc[name] = value
			return nil
		}
		return fmt.Errorf("cannot set index on root document")
	}

	// Navigate to the parent
	parent := types.AttributeValue(&types.AttributeValueMemberM{Value: doc})
	for i := 0; i < len(path.Parts)-1; i++ {
		part := path.Parts[i]
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			m, ok := parent.(*types.AttributeValueMemberM)
			if !ok {
				return fmt.Errorf("cannot access attribute %q on non-map type", name)
			}
			val, ok := m.Value[name]
			if !ok {
				// Create intermediate maps if they don't exist
				newMap := &types.AttributeValueMemberM{Value: make(map[string]types.AttributeValue)}
				m.Value[name] = newMap
				val = newMap
			}
			parent = val
		} else if part.Index != nil {
			l, ok := parent.(*types.AttributeValueMemberL)
			if !ok {
				return fmt.Errorf("cannot index non-list type at part %d", i)
			}
			idx := *part.Index
			if idx < 0 || idx >= len(l.Value) {
				return fmt.Errorf("index %d out of bounds", idx)
			}
			parent = l.Value[idx]
		}
	}

	// Set the final value
	lastPart := path.Parts[len(path.Parts)-1]
	if lastPart.Identifier != nil {
		name := lastPart.Identifier.GetName(names)
		m, ok := parent.(*types.AttributeValueMemberM)
		if !ok {
			return fmt.Errorf("cannot set attribute %q on non-map type", name)
		}
		m.Value[name] = value
	} else if lastPart.Index != nil {
		l, ok := parent.(*types.AttributeValueMemberL)
		if !ok {
			return fmt.Errorf("cannot set index on non-list type")
		}
		idx := *lastPart.Index
		if idx < 0 || idx >= len(l.Value) {
			return fmt.Errorf("index %d out of bounds", idx)
		}
		l.Value[idx] = value
	}

	return nil
}

func removePath(path *ast.AttributePath, names map[string]string, doc map[string]types.AttributeValue) error {
	if len(path.Parts) == 0 {
		return fmt.Errorf("empty path")
	}

	if len(path.Parts) == 1 {
		part := path.Parts[0]
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			delete(doc, name)
			return nil
		}
		return fmt.Errorf("cannot remove index from root document")
	}

	// Navigate to the parent
	parent := types.AttributeValue(&types.AttributeValueMemberM{Value: doc})
	for i := 0; i < len(path.Parts)-1; i++ {
		part := path.Parts[i]
		if part.Identifier != nil {
			name := part.Identifier.GetName(names)
			m, ok := parent.(*types.AttributeValueMemberM)
			if !ok {
				return fmt.Errorf("cannot access attribute %q on non-map type", name)
			}
			val, ok := m.Value[name]
			if !ok {
				// Path doesn't exist, nothing to remove
				return nil
			}
			parent = val
		} else if part.Index != nil {
			l, ok := parent.(*types.AttributeValueMemberL)
			if !ok {
				return fmt.Errorf("cannot index non-list type at part %d", i)
			}
			idx := *part.Index
			if idx < 0 || idx >= len(l.Value) {
				return nil // Index out of bounds, nothing to remove
			}
			parent = l.Value[idx]
		}
	}

	// Remove the final element
	lastPart := path.Parts[len(path.Parts)-1]
	if lastPart.Identifier != nil {
		name := lastPart.Identifier.GetName(names)
		m, ok := parent.(*types.AttributeValueMemberM)
		if !ok {
			return fmt.Errorf("cannot remove attribute %q from non-map type", name)
		}
		delete(m.Value, name)
	} else if lastPart.Index != nil {
		l, ok := parent.(*types.AttributeValueMemberL)
		if !ok {
			return fmt.Errorf("cannot remove index from non-list type")
		}
		idx := *lastPart.Index
		if idx >= 0 && idx < len(l.Value) {
			l.Value = append(l.Value[:idx], l.Value[idx+1:]...)
		}
	}

	return nil
}

func addToPath(path *ast.AttributePath, names map[string]string, doc map[string]types.AttributeValue, value types.AttributeValue) error {
	existing, err := getPathValue(path, names, doc)
	if err != nil {
		// If the attribute doesn't exist, SET it to the value
		return setPath(path, names, doc, value)
	}

	// ADD for numbers: adds the value to the existing number
	if existingNum, ok := existing.(*types.AttributeValueMemberN); ok {
		addNum, ok := value.(*types.AttributeValueMemberN)
		if !ok {
			return fmt.Errorf("ADD: cannot add non-number to number attribute")
		}
		existingVal, err := parseNumber(existingNum.Value)
		if err != nil {
			return err
		}
		addVal, err := parseNumber(addNum.Value)
		if err != nil {
			return err
		}
		result := &types.AttributeValueMemberN{Value: formatNumber(existingVal + addVal)}
		return setPath(path, names, doc, result)
	}

	// ADD for sets: adds elements to the set
	switch existingSet := existing.(type) {
	case *types.AttributeValueMemberSS:
		addSet, ok := value.(*types.AttributeValueMemberSS)
		if !ok {
			return fmt.Errorf("ADD: cannot add non-string-set to string set")
		}
		// Use a map to deduplicate
		m := make(map[string]struct{})
		for _, s := range existingSet.Value {
			m[s] = struct{}{}
		}
		for _, s := range addSet.Value {
			m[s] = struct{}{}
		}
		result := make([]string, 0, len(m))
		for s := range m {
			result = append(result, s)
		}
		return setPath(path, names, doc, &types.AttributeValueMemberSS{Value: result})

	case *types.AttributeValueMemberNS:
		addSet, ok := value.(*types.AttributeValueMemberNS)
		if !ok {
			return fmt.Errorf("ADD: cannot add non-number-set to number set")
		}
		m := make(map[string]struct{})
		for _, s := range existingSet.Value {
			m[s] = struct{}{}
		}
		for _, s := range addSet.Value {
			m[s] = struct{}{}
		}
		result := make([]string, 0, len(m))
		for s := range m {
			result = append(result, s)
		}
		return setPath(path, names, doc, &types.AttributeValueMemberNS{Value: result})

	case *types.AttributeValueMemberBS:
		addSet, ok := value.(*types.AttributeValueMemberBS)
		if !ok {
			return fmt.Errorf("ADD: cannot add non-binary-set to binary set")
		}
		// For binary sets, we need to compare byte slices
		result := make([][]byte, len(existingSet.Value))
		copy(result, existingSet.Value)
		for _, b := range addSet.Value {
			found := false
			for _, existing := range result {
				if bytesEqual(existing, b) {
					found = true
					break
				}
			}
			if !found {
				result = append(result, b)
			}
		}
		return setPath(path, names, doc, &types.AttributeValueMemberBS{Value: result})

	default:
		return fmt.Errorf("ADD: unsupported attribute type %T", existing)
	}
}

func deleteFromPath(path *ast.AttributePath, names map[string]string, doc map[string]types.AttributeValue, value types.AttributeValue) error {
	existing, err := getPathValue(path, names, doc)
	if err != nil {
		// If attribute doesn't exist, nothing to delete
		return nil
	}

	// DELETE is only for sets
	switch existingSet := existing.(type) {
	case *types.AttributeValueMemberSS:
		deleteSet, ok := value.(*types.AttributeValueMemberSS)
		if !ok {
			return fmt.Errorf("DELETE: cannot delete non-string-set from string set")
		}
		toDelete := make(map[string]struct{})
		for _, s := range deleteSet.Value {
			toDelete[s] = struct{}{}
		}
		result := make([]string, 0)
		for _, s := range existingSet.Value {
			if _, found := toDelete[s]; !found {
				result = append(result, s)
			}
		}
		return setPath(path, names, doc, &types.AttributeValueMemberSS{Value: result})

	case *types.AttributeValueMemberNS:
		deleteSet, ok := value.(*types.AttributeValueMemberNS)
		if !ok {
			return fmt.Errorf("DELETE: cannot delete non-number-set from number set")
		}
		toDelete := make(map[string]struct{})
		for _, s := range deleteSet.Value {
			toDelete[s] = struct{}{}
		}
		result := make([]string, 0)
		for _, s := range existingSet.Value {
			if _, found := toDelete[s]; !found {
				result = append(result, s)
			}
		}
		return setPath(path, names, doc, &types.AttributeValueMemberNS{Value: result})

	case *types.AttributeValueMemberBS:
		deleteSet, ok := value.(*types.AttributeValueMemberBS)
		if !ok {
			return fmt.Errorf("DELETE: cannot delete non-binary-set from binary set")
		}
		result := make([][]byte, 0)
		for _, existing := range existingSet.Value {
			found := false
			for _, toDelete := range deleteSet.Value {
				if bytesEqual(existing, toDelete) {
					found = true
					break
				}
			}
			if !found {
				result = append(result, existing)
			}
		}
		return setPath(path, names, doc, &types.AttributeValueMemberBS{Value: result})

	default:
		return fmt.Errorf("DELETE: unsupported attribute type %T", existing)
	}
}

func parseNumber(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

func formatNumber(f float64) string {
	// Format without trailing zeros
	s := fmt.Sprintf("%g", f)
	return s
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// validateNoPathOverlap checks that no paths overlap across different clause types.
// DynamoDB rejects expressions like "SET a = :v REMOVE a" with
// "Two document paths overlap with each other".
// Same-clause duplicates are allowed (second wins).
func validateNoPathOverlap(expr *ast.UpdateExpression, names map[string]string) error {
	type pathEntry struct {
		path      string
		clauseIdx int // 0=SET, 1=REMOVE, 2=ADD, 3=DELETE
	}

	clauseNames := []string{"SET", "REMOVE", "ADD", "DELETE"}
	var allPaths []pathEntry

	// Collect paths from SET actions
	for _, action := range expr.SetActions {
		allPaths = append(allPaths, pathEntry{
			path:      resolvePath(action.Path, names),
			clauseIdx: 0,
		})
	}

	// Collect paths from REMOVE actions
	for _, action := range expr.RemoveActions {
		allPaths = append(allPaths, pathEntry{
			path:      resolvePath(action.Path, names),
			clauseIdx: 1,
		})
	}

	// Collect paths from ADD actions
	for _, action := range expr.AddActions {
		allPaths = append(allPaths, pathEntry{
			path:      resolvePath(action.Path, names),
			clauseIdx: 2,
		})
	}

	// Collect paths from DELETE actions
	for _, action := range expr.DeleteActions {
		allPaths = append(allPaths, pathEntry{
			path:      resolvePath(action.Path, names),
			clauseIdx: 3,
		})
	}

	// Check for overlaps across different clause types
	for i := 0; i < len(allPaths); i++ {
		for j := i + 1; j < len(allPaths); j++ {
			if allPaths[i].clauseIdx == allPaths[j].clauseIdx {
				// Same clause type - allowed (second wins)
				continue
			}
			if pathsOverlap(allPaths[i].path, allPaths[j].path) {
				return fmt.Errorf(
					"Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [%s] via %s clause, path two: [%s] via %s clause",
					allPaths[i].path, clauseNames[allPaths[i].clauseIdx],
					allPaths[j].path, clauseNames[allPaths[j].clauseIdx],
				)
			}
		}
	}

	return nil
}

// resolvePath converts an AttributePath to a canonical string representation.
func resolvePath(path *ast.AttributePath, names map[string]string) string {
	var parts []string
	for _, part := range path.Parts {
		if part.Index != nil {
			parts = append(parts, fmt.Sprintf("[%d]", *part.Index))
		} else if part.Identifier != nil {
			parts = append(parts, part.Identifier.GetName(names))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	// Build path string: first part, then .name or [idx] for rest
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		if parts[i][0] == '[' {
			result += parts[i]
		} else {
			result += "." + parts[i]
		}
	}
	return result
}

// pathsOverlap checks if two paths overlap (one is a prefix of the other).
// For example, "a.b" and "a.b.c" overlap, but "a.b" and "a.c" do not.
func pathsOverlap(path1, path2 string) bool {
	// Exact match
	if path1 == path2 {
		return true
	}

	// Check if one is a prefix of the other
	// Need to be careful: "a" should overlap with "a.b" and "a[0]", but not "ab"
	shorter, longer := path1, path2
	if len(path1) > len(path2) {
		shorter, longer = path2, path1
	}

	if len(shorter) == 0 {
		return false
	}

	// Check if longer starts with shorter followed by '.' or '['
	if len(longer) > len(shorter) &&
		longer[:len(shorter)] == shorter &&
		(longer[len(shorter)] == '.' || longer[len(shorter)] == '[') {
		return true
	}

	return false
}
