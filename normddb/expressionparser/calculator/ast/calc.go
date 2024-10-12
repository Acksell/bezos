package ast

import (
	"fmt"
	"strconv"
)

type Node interface {
	Eval() int
}

func NewIntExpr(b []byte) IntExpr {
	i, err := strconv.Atoi(string(b))
	if err != nil {
		panic(err)
	}
	return IntExpr(i)
}

func NewTermExpr(factor, rest any) *TermExpr {
	f, ok := factor.(*FactorExpr)
	if !ok {
		panic(fmt.Sprintf("termexpr failed typecast of fact: got %T want %T", factor, f))
	}

	var parts []*TermPart
	anyParts := rest.([]any)
	for p := range anyParts {
		part, ok := anyParts[p].(*TermPart)
		if !ok {
			panic(fmt.Sprintf("termexpr failed typecast of termpart: got %T want %T", rest, parts))
		}
		parts = append(parts, part)
	}
	return &TermExpr{
		Term: f,
		Rest: parts,
	}
}

func NewFactorExpr(node, rest any) *FactorExpr {
	n, ok := node.(Node)
	if !ok {
		panic(fmt.Sprintf("factorexpr failed typecast of node: got %T want %T", n, node))
	}
	var parts []*FactorPart
	anyParts := rest.([]any)
	for p := range anyParts {
		part, ok := anyParts[p].(*FactorPart)
		if !ok {
			panic(fmt.Sprintf("factorexpr failed typecast of factorpart: got %T want %T", rest, parts))
		}
		parts = append(parts, part)
	}
	return &FactorExpr{
		Factor: n,
		Rest:   parts,
	}
}

func NewFactorPart(op any, fact any) *FactorPart {
	f, ok := fact.(Node)
	if !ok {
		panic(fmt.Sprintf("factorpart failed typecast of fact: got %T want %T", fact, f))
	}
	mulOp, ok := op.(MulOp)
	if !ok {
		panic(fmt.Sprintf("factorpart failed typecast of mulop: got %T want %T", op, mulOp))
	}
	return &FactorPart{
		Op:   mulOp,
		Fact: f,
	}
}

func NewTermPart(op any, term any) *TermPart {
	t, ok := term.(Node)
	if !ok {
		panic(fmt.Sprintf("termpart failed typecast of term: got %T want %T", term, t))
	}
	addOp, ok := op.(AddOp)
	if !ok {
		panic(fmt.Sprintf("termpart failed typecast of addop: got %T want %T", op, addOp))
	}
	return &TermPart{
		AddOp: addOp,
		Term:  t,
	}
}

type IntExpr int

func (i IntExpr) Eval() int {
	return int(i)
}

type TermExpr struct {
	Term Node
	Rest []*TermPart
}

func (a *TermExpr) Eval() int {
	acc := a.Term.Eval()
	for _, part := range a.Rest {
		switch part.AddOp {
		case AddOpAdd:
			acc += part.Term.Eval()
		case AddOpSub:
			acc -= part.Term.Eval()
		}
	}
	return acc
}

type TermPart struct {
	AddOp AddOp
	Term  Node
}

type AddOp string

const (
	AddOpAdd AddOp = "+"
	AddOpSub AddOp = "-"
)

type FactorExpr struct {
	Factor Node
	Rest   []*FactorPart
}

func (m *FactorExpr) Eval() int {
	acc := m.Factor.Eval()
	for _, part := range m.Rest {
		switch part.Op {
		case MulOpMul:
			acc *= part.Fact.Eval()
		case MulOpDiv:
			acc /= part.Fact.Eval()
		}
	}
	return acc
}

type FactorPart struct {
	Op   MulOp
	Fact Node
}

type MulOp string

const (
	MulOpMul MulOp = "*"
	MulOpDiv MulOp = "/"
)
