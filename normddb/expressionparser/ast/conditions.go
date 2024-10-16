package ast

import (
	"fmt"
	"norm/normddb/expressionparser/astutil"
)

type Input struct {
	Document         map[string]AttributeValue
	ExpressionNames  map[string]string
	ExpressionValues map[string]AttributeValue
}

// Condition is the root interface for all AST nodes for Conditions.
//
// DB uses parser for conditionexpression
// which returns this AST to use for
// interpreting and evaluating the condition
// against a given DDB document
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
		fmt.Printf("left: %T%+v right: %T%+v (%T) eq: %v", left, left, right, right, right.Value, left.Value == right.Value)
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
	parts := []*AttributePathPart{
		NewAttributePathPart(head),
	}
	switch t := tail.(type) {
	case string:
		parts = append(parts, NewAttributePathPart(tail))
	case int:
		parts = append(parts, NewAttributePathPart(tail))
	case []any:
		for _, v := range t {
			parts = append(parts, NewAttributePathPart(v))
		}
	}
	return &AttributePath{Parts: parts}
}

// AttributePath represents a document path (e.g., "user.profile.age")
type AttributePath struct {
	Parts []*AttributePathPart
}

// Ensure AttributePath implements Expression
func (p *AttributePath) GetValue(input Input) *Operand {
	v, exists := resolvePath(p.Parts, input)
	if !exists {
		panic(fmt.Sprintf("attribute %s not found", FullPath(p.Parts, input)))
	}
	return &Operand{Value: v.Value, Type: v.Type}
}

// only for debugging
func FullPath(parts []*AttributePathPart, input Input) string {
	var path string
	for _, part := range parts {
		if part.Identifier != nil {
			path += part.Identifier.Resolve(input)
		} else if part.Index != nil {
			path += fmt.Sprintf("[%d]", *part.Index)
		} else {
			panic("both Identifier and Index are nil")
		}
	}
	return path
}

func NewAttributePathPart(p any) *AttributePathPart {
	switch v := p.(type) {
	case string:
		return &AttributePathPart{Identifier: &Identifier{Name: &v}}
	case int:
		return &AttributePathPart{Index: &v}
	case *ExpressionAttributeName:
		return &AttributePathPart{Identifier: &Identifier{NameExpression: v}}
	default:
		panic(fmt.Sprintf("unsupported path part %T", p))
	}
}

type AttributePathPart struct {
	// only one is non-nil
	Identifier *Identifier
	Index      *int
}

func (p *AttributePathPart) toString(input Input) string {
	if p.Identifier == nil {
		panic("called toString on nil Identifier")
	}
	return p.Identifier.Resolve(input)
}

func (p *AttributePathPart) toInt() int {
	if p.Index == nil {
		panic("got nil index")
	}
	return *p.Index
}

func resolvePath(path []*AttributePathPart, input Input) (AttributeValue, bool) {
	if len(path) == 0 {
		panic("empty path")
	}

	// start with the document
	var attr AttributeValue
	var exists bool
	traversed := path[0].toString(input)
	if attr, exists = input.Document[traversed]; !exists {
		panic(fmt.Sprintf("attribute %s not found", traversed))
	}

	// follow the path
	for i := 1; i < len(path); i++ {
		part := path[i]
		switch attr.Type {
		case MAP:
			next := part.toString(input)
			fmt.Println("next:", next)
			attr, exists = astutil.CastTo[map[string]AttributeValue](attr.Value)[next]
			traversed += "." + next
			if !exists {
				panic(fmt.Sprintf("attribute %s not found", traversed))
			}
		case LIST:
			idx := part.toInt()
			val := astutil.CastTo[[]AttributeValue](attr.Value)
			traversed += fmt.Sprintf("[%d]", idx)
			if idx < 0 || idx >= len(val) {
				panic(fmt.Sprintf("index %d out of bounds on %s", idx, traversed))
			}
			attr = val[idx]
		default:
			panic(fmt.Sprintf("unresolved path, attribute at path %s is not map or list got %T", traversed, attr.Value))
		}
	}

	return attr, exists
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

// implement both GetValue and Eval, but panic
// if the function is used in the wrong context.
// So name=="size" can only be used in GetValue
// and name!="size" can only be used in Eval.
// This is done instead of an explicit grammar
// for more custom error messages.
func (f *FunctionCall) GetValue(input Input) *Operand {
	switch f.FunctionName {
	case "size":
		path := astutil.CastTo[*AttributePath](f.Args[0], "size first arg")
		// path
		attr, exists := resolvePath(path.Parts, input)
		if !exists {
			panic(fmt.Sprintf("attribute %s not found", FullPath(path.Parts, input)))
		}
		switch attr.Type {
		case STRING:
			return &Operand{len(attr.Value.(string)), NUMBER}
		case NUMBER:
			panic("size function not supported for number")
		case LIST, NUMBER_SET, STRING_SET, BINARY_SET:
			return &Operand{len(attr.Value.([]any)), NUMBER}
		case MAP:
			return &Operand{len(attr.Value.(map[string]any)), NUMBER}
		case BINARY:
			return &Operand{len(attr.Value.([]byte)), NUMBER}
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
		path := astutil.CastTo[*AttributePath](f.Args[0], "attribute_exist first arg")
		fmt.Println("attribute_exists path:", FullPath(path.Parts, input))
		_, exists := resolvePath(path.Parts, input)
		return exists
	case "begins_with":
		path := astutil.CastTo[*AttributePath](f.Args[0])
		attr, exists := resolvePath(path.Parts, input)
		if !exists {
			panic(fmt.Sprintf("attribute %s not found", FullPath(path.Parts, input)))
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

type Identifier struct {
	Name           *string
	NameExpression *ExpressionAttributeName
}

func (i *Identifier) Resolve(input Input) string {
	if i.Name != nil {
		return *i.Name
	}

	if i.NameExpression == nil {
		panic("got both nil NameExpression and Identifier")
	}
	return i.NameExpression.resolve(input)
}

type ExpressionAttributeName struct {
	Name string
}

func (n *ExpressionAttributeName) resolve(input Input) string {
	fmt.Println("trying to resolve expression attribute name", n.Name)
	v, found := input.ExpressionNames[n.Name]
	if !found {
		panic(fmt.Sprintf("expression attribute value %s not found", n.Name))
	}
	fmt.Println("RESOLVED", n.Name, "to", v)
	return v
}

func NewExpressionAttributeValue(name string) *ExpressionAttributeValue {
	return &ExpressionAttributeValue{Name: name}
}

type ExpressionAttributeValue struct {
	Name string
}

func (v *ExpressionAttributeValue) GetValue(input Input) *Operand {
	val, found := input.ExpressionValues[v.Name]
	if !found {
		panic(fmt.Sprintf("expression attribute value %s not found", v.Name))
	}
	return &Operand{Value: val.Value, Type: val.Type}
}
