package keyconditionast

import (
	"bezos/dynamodb/ddbstore/expressionparser/astutil"
	"fmt"
)

func (left *KeyValue) Equal(right *KeyValue) bool {
	if left.Type != right.Type {
		panic("cannot compare different types")
	}
	switch left.Type {
	case NUMBER, STRING, BINARY:
		return left.Value == right.Value
	default:
		panic(fmt.Sprintf("unsupported key type %s", left.Type))
	}
}

func (left *KeyValue) LessThan(right *KeyValue) bool {
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

func (left *KeyValue) LessThanOrEqual(right *KeyValue) bool {
	return left.LessThan(right) || left.Equal(right)
}

func (left *KeyValue) GreaterThanOrEqual(right *KeyValue) bool {
	return !left.LessThan(right)
}

func (left *KeyValue) GreaterThan(right *KeyValue) bool {
	return !left.LessThanOrEqual(right)
}
