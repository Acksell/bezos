// Package ast contains the AST types for DynamoDB UpdateExpression parsing.
//
// UpdateExpressions have the following structure:
//
//	[SET action [, action] ...]
//	[REMOVE path [, path] ...]
//	[ADD path value [, path value] ...]
//	[DELETE path value [, path value] ...]
//
// SET actions:
//
//	path = value
//	path = operand + operand  (numeric addition or list concatenation)
//	path = operand - operand  (numeric subtraction)
//	path = if_not_exists(path, value)
//	path = list_append(list1, list2)
package ast

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/astutil"
)

// UpdateExpression represents the full parsed update expression.
type UpdateExpression struct {
	SetActions    []SetAction
	RemoveActions []RemovePath
	AddActions    []AddAction
	DeleteActions []DeleteAction
}

// SetAction represents a single SET action: path = value
type SetAction struct {
	Path  *AttributePath
	Value SetValue
}

// SetValue is the interface for values on the right side of SET actions.
type SetValue interface {
	setValueMarker()
}

// Operand represents a value operand in an expression.
// Can be an attribute path, a literal value, or a function call.
type Operand interface {
	SetValue
	operandMarker()
}

// ArithmeticOp represents an arithmetic operation: operand + operand or operand - operand
type ArithmeticOp struct {
	Left     Operand
	Operator string // "+" or "-"
	Right    Operand
}

func (a *ArithmeticOp) setValueMarker() {}

func NewArithmeticOp(left any, op string, right any) *ArithmeticOp {
	l := astutil.CastTo[Operand](left, "ArithmeticOp.Left")
	r := astutil.CastTo[Operand](right, "ArithmeticOp.Right")
	return &ArithmeticOp{Left: l, Operator: op, Right: r}
}

// IfNotExists represents: if_not_exists(path, value)
type IfNotExists struct {
	Path  *AttributePath
	Value Operand
}

func (i *IfNotExists) setValueMarker() {}
func (i *IfNotExists) operandMarker()  {}

func NewIfNotExists(path, value any) *IfNotExists {
	p := astutil.CastTo[*AttributePath](path, "IfNotExists.Path")
	v := astutil.CastTo[Operand](value, "IfNotExists.Value")
	return &IfNotExists{Path: p, Value: v}
}

// ListAppend represents: list_append(list1, list2)
type ListAppend struct {
	List1 Operand
	List2 Operand
}

func (l *ListAppend) setValueMarker() {}
func (l *ListAppend) operandMarker()  {}

func NewListAppend(list1, list2 any) *ListAppend {
	l1 := astutil.CastTo[Operand](list1, "ListAppend.List1")
	l2 := astutil.CastTo[Operand](list2, "ListAppend.List2")
	return &ListAppend{List1: l1, List2: l2}
}

// RemovePath represents a REMOVE action path.
type RemovePath struct {
	Path *AttributePath
}

// AddAction represents an ADD action: path value (for numbers/sets)
type AddAction struct {
	Path  *AttributePath
	Value Operand
}

// DeleteAction represents a DELETE action: path value (for sets)
type DeleteAction struct {
	Path  *AttributePath
	Value Operand
}

// AttributePath represents a document path (e.g., "user.profile[0].age")
type AttributePath struct {
	Parts []*AttributePathPart
}

func (a *AttributePath) setValueMarker() {}
func (a *AttributePath) operandMarker()  {}

func NewAttributePath(head any, tail any) *AttributePath {
	parts := []*AttributePathPart{
		newAttributePathPart(head),
	}
	switch t := tail.(type) {
	case string:
		parts = append(parts, newAttributePathPart(tail))
	case int:
		parts = append(parts, newAttributePathPart(tail))
	case []any:
		for _, v := range t {
			parts = append(parts, newAttributePathPart(v))
		}
	}
	return &AttributePath{Parts: parts}
}

func newAttributePathPart(p any) *AttributePathPart {
	switch v := p.(type) {
	case string:
		if astutil.IsReservedName(v) {
			panic(fmt.Sprintf("attribute name %q is reserved, use ExpressionAttributeNames instead", v))
		}
		return &AttributePathPart{Identifier: &Identifier{Name: &v}}
	case int:
		return &AttributePathPart{Index: &v}
	case *ExpressionAttributeName:
		return &AttributePathPart{Identifier: &Identifier{NameExpression: v}}
	default:
		panic(fmt.Sprintf("unsupported path part type %T", p))
	}
}

// AttributePathPart represents either an identifier or an index.
type AttributePathPart struct {
	Identifier *Identifier
	Index      *int
}

// Identifier represents either a raw name or an expression attribute name.
type Identifier struct {
	Name           *string
	NameExpression *ExpressionAttributeName
}

func (i *Identifier) GetName(names map[string]string) string {
	if i.Name != nil {
		return *i.Name
	}
	if i.NameExpression == nil {
		panic("both Name and NameExpression are nil")
	}
	return i.NameExpression.Resolve(names)
}

// ExpressionAttributeName represents a #name placeholder.
type ExpressionAttributeName struct {
	Alias string
}

func (n *ExpressionAttributeName) Resolve(names map[string]string) string {
	name, found := names[n.Alias]
	if !found {
		panic(fmt.Sprintf("expression attribute name %s not found", n.Alias))
	}
	return name
}

func NewExpressionAttributeName(alias string) *ExpressionAttributeName {
	return &ExpressionAttributeName{Alias: alias}
}

// ExpressionAttributeValue represents a :value placeholder.
type ExpressionAttributeValue struct {
	Alias string
}

func (v *ExpressionAttributeValue) setValueMarker() {}
func (v *ExpressionAttributeValue) operandMarker()  {}

func NewExpressionAttributeValue(alias string) *ExpressionAttributeValue {
	return &ExpressionAttributeValue{Alias: alias}
}

// NewUpdateExpression creates an UpdateExpression from the parsed clauses.
func NewUpdateExpression(clauses any) *UpdateExpression {
	expr := &UpdateExpression{}
	switch c := clauses.(type) {
	case []any:
		for _, clause := range c {
			expr.mergeClause(clause)
		}
	default:
		expr.mergeClause(clauses)
	}
	return expr
}

func (e *UpdateExpression) mergeClause(clause any) {
	switch c := clause.(type) {
	case *setClause:
		e.SetActions = append(e.SetActions, c.Actions...)
	case *removeClause:
		e.RemoveActions = append(e.RemoveActions, c.Paths...)
	case *addClause:
		e.AddActions = append(e.AddActions, c.Actions...)
	case *deleteClause:
		e.DeleteActions = append(e.DeleteActions, c.Actions...)
	case *UpdateExpression:
		e.SetActions = append(e.SetActions, c.SetActions...)
		e.RemoveActions = append(e.RemoveActions, c.RemoveActions...)
		e.AddActions = append(e.AddActions, c.AddActions...)
		e.DeleteActions = append(e.DeleteActions, c.DeleteActions...)
	}
}

// Internal clause types used during parsing
type setClause struct {
	Actions []SetAction
}

type removeClause struct {
	Paths []RemovePath
}

type addClause struct {
	Actions []AddAction
}

type deleteClause struct {
	Actions []DeleteAction
}

func NewSetClause(actions any) *setClause {
	return &setClause{Actions: astutil.CastTo[[]SetAction](actions, "SetClause.Actions")}
}

func NewRemoveClause(paths any) *removeClause {
	return &removeClause{Paths: astutil.CastTo[[]RemovePath](paths, "RemoveClause.Paths")}
}

func NewAddClause(actions any) *addClause {
	return &addClause{Actions: astutil.CastTo[[]AddAction](actions, "AddClause.Actions")}
}

func NewDeleteClause(actions any) *deleteClause {
	return &deleteClause{Actions: astutil.CastTo[[]DeleteAction](actions, "DeleteClause.Actions")}
}

func NewSetAction(path, value any) SetAction {
	p := astutil.CastTo[*AttributePath](path, "SetAction.Path")
	v := astutil.CastTo[SetValue](value, "SetAction.Value")
	return SetAction{Path: p, Value: v}
}

func NewRemovePath(path any) RemovePath {
	p := astutil.CastTo[*AttributePath](path, "RemovePath.Path")
	return RemovePath{Path: p}
}

func NewAddAction(path, value any) AddAction {
	p := astutil.CastTo[*AttributePath](path, "AddAction.Path")
	v := astutil.CastTo[Operand](value, "AddAction.Value")
	return AddAction{Path: p, Value: v}
}

func NewDeleteAction(path, value any) DeleteAction {
	p := astutil.CastTo[*AttributePath](path, "DeleteAction.Path")
	v := astutil.CastTo[Operand](value, "DeleteAction.Value")
	return DeleteAction{Path: p, Value: v}
}
