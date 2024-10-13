package ast

import (
	"fmt"
	"norm/normddb/expressionparser/astutil"
)

// DB uses parser for conditionexpression
// which returns this AST to use for
// interpreting and evaluating the condition
// against a given DDB document

type tableDefinition struct { // part of input
	// todo, what needs to be here? is it needed at all?
}

type Input struct {
	Document         map[string]AttributeValue
	ExpressionNames  map[string]string
	ExpressionValues map[string]AttributeValue
}

// Condition is the root interface for all AST nodes for Conditions.
type Condition interface {
	Eval(input Input) bool
}

// Expression is an interface for that is used as operands in comparisons and function calls.
type Expression interface {
	// GetValue returns an operand to be used in conditions
	GetValue(input Input) *Operand
}

type AttributeValue struct {
	Value any
	Type  AttributeType
}

type Operand struct {
	Value any
	Type  AttributeType
}

func (left *Operand) Equal(right *Operand) bool {
	if left.Type != right.Type {
		panic("cannot compare different types")
	}
	switch left.Type {
	// all DynamoDB lists & sets are encoded as strings
	case LIST, NUMBER_SET, STRING_SET, BINARY_SET:
		av1 := astutil.ToSlice[string](left.Value)
		av2 := astutil.ToSlice[string](left.Value)
		if len(av1) != len(av2) {
			return false
		}
		for i := range av1 {
			if av1[i] != av2[i] {
				return false
			}
		}
		return true
	case NUMBER, STRING, BINARY, BOOL, NULL:
		return left.Value == right.Value
	case MAP:
		panic("cannot compare two maps")
	default:
		panic(fmt.Sprintf("unsupported attribute type %s", left.Type))
	}
}

func (left *Operand) LessThan(right *Operand) bool {
	if left.Type != right.Type {
		panic("cannot compare different types")
	}
	switch left.Type {
	case NUMBER:
		return astutil.Float64(left.Value) < astutil.Float64(right.Value)
	// binary is also stored as string
	case STRING, BINARY:
		return astutil.String(left.Value) < astutil.String(right.Value)
	default:
		panic(fmt.Sprintf("unsupported attribute type %s", left.Type))
	}
}

func (left *Operand) LessThanOrEqual(right *Operand) bool {
	return left.LessThan(right) || left.Equal(right)
}

func (left *Operand) GreaterThanOrEqual(right *Operand) bool {
	return !left.LessThan(right)
}

func (left *Operand) GreaterThan(right *Operand) bool {
	return !left.LessThanOrEqual(right)
}

func NewComparison(operator, left, right any) *Comparison {
	op := astutil.String(operator)
	l := astutil.CastTo[Expression](left)
	r := astutil.CastTo[Expression](right)
	return &Comparison{Operator: op, Left: l, Right: r}
}

// Comparison represents binary comparison operations (e.g. =, <, >)
type Comparison struct {
	Operator string     // e.g., "=" or ">"
	Left     Expression // left operand
	Right    Expression // right operand
}

func (c *Comparison) Eval(input Input) bool {
	leftVal := c.Left.GetValue(input)
	rightVal := c.Right.GetValue(input)

	switch c.Operator {
	case "=":
		return leftVal.Equal(rightVal)
	case "<>":
		return !leftVal.Equal(rightVal)
	case ">":
		// todo use generics. This assumes float comparison
		return leftVal.GreaterThan(rightVal)
	case "<":
		return leftVal.LessThan(rightVal)
	case ">=":
		return leftVal.GreaterThanOrEqual(rightVal)
	case "<=":
		return leftVal.LessThanOrEqual(rightVal)
	default:
		panic(fmt.Sprintf("unsupported operator %s", c.Operator))
	}
}

func NewBetweenExpr(val, low, high any) *BetweenExpr {
	v := astutil.CastTo[Expression](val)
	lo := astutil.CastTo[Expression](low)
	hi := astutil.CastTo[Expression](high)
	return &BetweenExpr{v, lo, hi}
}

func (b *BetweenExpr) Eval(input Input) bool {
	lo := b.Low.GetValue(input)
	hi := b.High.GetValue(input)
	if lo.GreaterThan(hi) {
		panic(fmt.Sprintf("low must be less than or equal to high: got %v > %v", b.Low, b.High))
	}
	val := b.Val.GetValue(input)
	return val.GreaterThanOrEqual(lo) && val.LessThanOrEqual(hi)
}

type BetweenExpr struct {
	Val  Expression
	Low  Expression
	High Expression
}

func NewContainsExpr(container, val any) *ContainsExpr {
	c := astutil.CastTo[[]Expression](container)
	v := astutil.CastTo[Expression](val)
	return &ContainsExpr{c, v}
}

func (c *ContainsExpr) Eval(input Input) bool {
	val := c.Val.GetValue(input)
	for _, item := range c.Container {
		if item.GetValue(input) == val {
			return true
		}
	}
	return false
}

type ContainsExpr struct {
	Container []Expression
	Val       Expression
}

func NewAndCondition(left, right any) *LogicalOp {
	l := astutil.CastTo[Condition](left)
	r := astutil.CastTo[Condition](right)
	return &LogicalOp{Operator: "AND", Left: l, Right: r}
}

func NewOrCondition(left, right any) *LogicalOp {
	l := astutil.CastTo[Condition](left)
	r := astutil.CastTo[Condition](right)
	return &LogicalOp{Operator: "OR", Left: l, Right: r}
}

func NewNotCondition(cond any) *LogicalOp {
	c := astutil.CastTo[Condition](cond)
	return &LogicalOp{Operator: "NOT", Left: c}
}

// LogicalOp represents logical operations like AND, OR, NOT
type LogicalOp struct {
	Operator string // "AND", "OR", or "NOT"
	Left     Condition
	Right    Condition
}

func (l *LogicalOp) Eval(input Input) bool {
	switch l.Operator {
	case "AND":
		return l.Left.Eval(input) && l.Right.Eval(input)
	case "OR":
		return l.Left.Eval(input) || l.Right.Eval(input)
	case "NOT":
		return !l.Left.Eval(input)
	}
	return false
}

func NewAttributePathExpr(head, tail any) *AttributePath {
	h := astutil.String(head)
	t := astutil.CastTo[[]any](tail)
	path := &AttributePath{
		Parts: []AttributePathPart{
			{StringOrInt: h},
		},
	}
	for _, v := range t {
		path.Parts = append(path.Parts, AttributePathPart{StringOrInt: v})
	}
	return path
}

// AttributePath represents a document path (e.g., "user.profile.age")
type AttributePath struct {
	Parts []AttributePathPart
}

type AttributePathPart struct {
	StringOrInt any
}

func resolvePath(path []AttributePathPart, doc map[string]AttributeValue) (AttributeValue, bool) {
	// todo implement
	return AttributeValue{}, false
}

// Ensure AttributePath implements Expression
func (p *AttributePath) GetValue(input Input) any {
	v, exists := resolvePath(p.Parts, input.Document)
	if !exists {
		panic(fmt.Sprintf("attribute %s not found", p.Parts))
	}
	return &v
}

type AttributeType string

const (
	STRING     AttributeType = "S"
	NUMBER     AttributeType = "N"
	BINARY     AttributeType = "B"
	NUMBER_SET AttributeType = "NS"
	STRING_SET AttributeType = "SS"
	BINARY_SET AttributeType = "BS"
	BOOL       AttributeType = "BOOL"
	NULL       AttributeType = "NULL"
	LIST       AttributeType = "L"
	MAP        AttributeType = "M"
)

type funcDef struct {
	nArgs int
	typ   AttributeType
}

var funcs = map[string]funcDef{
	"attribute_exists":     {nArgs: 1, typ: BOOL},
	"attribute_not_exists": {nArgs: 1, typ: BOOL},
	"attribute_type":       {nArgs: 2, typ: BOOL},
	"begins_with":          {nArgs: 2, typ: BOOL},
	"contains":             {nArgs: 2, typ: BOOL},
	"size":                 {nArgs: 1, typ: NUMBER},
}

// FunctionCall represents function calls like "attribute_exists" or "begins_with"
type FunctionCall struct {
	FunctionName  string
	Args          []Expression
	AttributeType AttributeType
}

func NewFunctionCallExpr(namev, argsv any) *FunctionCall {
	name := astutil.String(namev)
	args := astutil.CastTo[[]Expression](argsv)
	def, found := funcs[name]
	if !found {
		panic(fmt.Sprintf("unknown function %s", name))
	}
	// if len(args) != def.nArgs {
	// 	panic(fmt.Sprintf("function %s expects %d arguments, got %d", name, def.nArgs, len(args)))
	// }
	return &FunctionCall{FunctionName: name, Args: args, AttributeType: def.typ}
}

// todo implement
// TODO update grammar to separate size and other functions, because size is an Expression
// and other functions are Conditions
// ALTERNATIVELY, implement both GetValue and Eval, but panic
// if the function is used in the wrong context.
// So name=="size" can only be used in GetValue
// and name!="size" can only be used in Eval.
func (f *FunctionCall) GetValue(input Input) any {
	switch f.FunctionName {
	case "size":
		path := astutil.CastTo[*AttributePath](f.Args[0])
		// path
		attr, exists := resolvePath(path.Parts, input.Document)
		if !exists {
			return 0
		}
		switch attr.Type {
		case STRING:
			return len(attr.Value.(string))
		case NUMBER:
			panic("size function not supported for number")
		case LIST, NUMBER_SET, STRING_SET, BINARY_SET:
			return len(attr.Value.([]any))
		case MAP:
			return len(attr.Value.(map[string]any))
		case BINARY:
			return len(attr.Value.([]byte))
		default:
			panic(fmt.Sprintf("unsupported attribute type %s", attr.Type))
		}
	default:
		panic(fmt.Sprintf(
			"The function is not allowed to be used this way in an expression; function: %s", f.FunctionName))
	}
}

func (f *FunctionCall) Eval(input Input) bool {
	switch f.FunctionName {
	case "attribute_exists":
		path := astutil.CastTo[*AttributePath](f.Args[0])
		_, exists := resolvePath(path.Parts, input.Document)
		return exists
	case "begins_with":
		path := astutil.CastTo[*AttributePath](f.Args[0])
		attr, exists := resolvePath(path.Parts, input.Document)
		if !exists {
			panic(fmt.Sprintf("attribute %s not found", path.Parts))
		}
		strVal := astutil.String(attr.Value)
		prefix := astutil.String(f.Args[1])
		return startsWith(strVal, prefix)
	}
	return false
}

func startsWith(str, prefix string) bool {
	return len(str) >= len(prefix) && str[:len(prefix)] == prefix
}

func NewExpressionAttributeName(name string) *ExpressionAttributeName {
	return &ExpressionAttributeName{Name: name}
}

type ExpressionAttributeName struct {
	Name string
}

func (e *ExpressionAttributeName) GetValue(input Input) *Operand {
	v, found := input.ExpressionNames[e.Name]
	if !found {
		panic(fmt.Sprintf("expression attribute name %s not found", e.Name))
	}
	return &Operand{Value: v, Type: STRING}
}

func NewExpressionAttributeValue(name string) *ExpressionAttributeValue {
	return &ExpressionAttributeValue{Name: name}
}

type ExpressionAttributeValue struct {
	Name string
}

func (e *ExpressionAttributeValue) GetValue(input Input) *Operand {
	v, found := input.ExpressionValues[e.Name]
	if !found {
		panic(fmt.Sprintf("expression attribute value %s not found", e.Name))
	}
	return &Operand{Value: v, Type: v.Type}
}
