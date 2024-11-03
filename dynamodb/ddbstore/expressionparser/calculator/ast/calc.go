package ast

import (
	"bezos/dynamodb/ddbstore/expressionparser/astutil"
)

type Node interface {
	Eval() int
}

func NewIntExpr(b []byte) IntExpr {
	i := astutil.Atoi(string(b))
	return IntExpr(i)
}

func NewTermExpr(factor, rest any) *TermExpr {
	f := astutil.CastTo[*FactorExpr](factor, "factor->*FactorExpr")
	parts := astutil.ToSlice[*TermPart](rest, "rest->[]*TermPart")
	return &TermExpr{
		Term: f,
		Rest: parts,
	}
}

func NewFactorExpr(node, rest any) *FactorExpr {
	n := astutil.CastTo[Node](node, "node->Node")
	parts := astutil.ToSlice[*FactorPart](rest, "rest->[]*FactorPart")
	return &FactorExpr{
		Factor: n,
		Rest:   parts,
	}
}

func NewFactorPart(op any, fact any) *FactorPart {
	f := astutil.CastTo[Node](fact, "fact->Node")
	mulOp := astutil.CastTo[MulOp](op, "op->MulOp")
	return &FactorPart{
		Op:   mulOp,
		Fact: f,
	}
}

func NewTermPart(op any, term any) *TermPart {
	t := astutil.CastTo[Node](term, "term->Node")
	addOp := astutil.CastTo[AddOp](op, "op->AddOp")
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
