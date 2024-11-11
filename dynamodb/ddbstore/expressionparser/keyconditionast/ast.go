/*
In this package we represent an AST for key conditions.
The package provides both the AST and the parser tree.
It does this via interfaces: The AST is represented by
the top level types and interfaces, but if you want to
dig into the underlying nodes, you can do so by
casting the interfaces to the appropriate types.

The parser is responsible for constructing the AST.
*/
package keyconditionast

import (
	"fmt"
)

// Struct representing the KeyCondition.
// Allows you to introspect the key condition in a strictly typed way.
// It's up to the user to verify the key names and types are valid for the table.
type KeyCondition struct {
	PartitionKeyCond PartitionKeyCondition
	SortKeyCond      *SortKeyCondition
}

func New(pk PartitionKeyCondition, sk *SortKeyCondition) *KeyCondition {
	return &KeyCondition{pk, sk}
}

type KeyValue struct {
	Value any
	Type  KeyType
}

type Value interface {
	GetValue() KeyValue
}

type Identifier interface {
	GetName() string
}

type KeyType string

const (
	STRING KeyType = "S"
	NUMBER KeyType = "N"
	BINARY KeyType = "B"
)

type PartitionKeyCondition struct {
	KeyName     Identifier
	EqualsValue Value
}

func NewPartitionKeyCondition(name Identifier, eq Value) PartitionKeyCondition {
	return PartitionKeyCondition{name, eq}
}

type SortKeyCondition struct {
	// only one is nil
	Between    *KeyBetween
	BeginsWith *KeyBeginsWith
	Compare    *KeyComparison
}

func (c *SortKeyCondition) KeyName() string {
	switch {
	case c.Between != nil:
		return c.Between.KeyName.GetName()
	case c.BeginsWith != nil:
		return c.BeginsWith.KeyName.GetName()
	case c.Compare != nil:
		return c.Compare.KeyName.GetName()
	default:
		panic("no key name found")
	}
}

func NewBetweenCondition(key Identifier, lower, upper Value) *SortKeyCondition {
	if lo, hi := lower.GetValue().Type, upper.GetValue().Type; lo != hi {
		panic(fmt.Sprintf("lower and upper must be the same type for BETWEEN condition, got %T and %T", lo, hi))
	}
	if lo, hi := lower.GetValue(), upper.GetValue(); hi.LessThan(&lo) {
		panic(fmt.Sprintf("low must be less than or equal to high: got %v > %v", lo, hi))
	}
	return &SortKeyCondition{Between: &KeyBetween{key, lower, upper}}
}

type KeyBetween struct {
	KeyName Identifier
	Lower   Value
	Upper   Value
}

func NewBeginsWithCondition(key Identifier, prefix Value) *SortKeyCondition {
	if got, want := prefix.GetValue().Type, STRING; got != want {
		panic(fmt.Sprintf("BeginsWith prefix must be a string, got %v want %v, for value %v", got, want, prefix.GetValue().Value))
	}
	return &SortKeyCondition{BeginsWith: &KeyBeginsWith{key, prefix}}
}

type KeyBeginsWith struct {
	KeyName Identifier
	Prefix  Value
}

func NewComparisonCondition(key Identifier, comp KeyComparator, value Value) *SortKeyCondition {
	return &SortKeyCondition{Compare: &KeyComparison{key, comp, value}}
}

type KeyComparison struct {
	KeyName Identifier
	Comp    KeyComparator
	Value   Value
}

type KeyComparator string

const (
	Equal          KeyComparator = "="
	LessThan       KeyComparator = "<"
	LessOrEqual    KeyComparator = "<="
	GreaterThan    KeyComparator = ">"
	GreaterOrEqual KeyComparator = ">="
)

func NewRawName(name string) *RawName {
	return &RawName{name}
}

type RawName struct {
	Name string
}

func (n *RawName) GetName() string {
	return n.Name
}

func NewExpressionAttributeName(alias, resolved string) *ExpressionAttributeName {
	return &ExpressionAttributeName{alias, resolved}
}

type ExpressionAttributeName struct {
	Alias    string
	Resolved string
}

func (n *ExpressionAttributeName) GetName() string {
	return n.Resolved
}

func NewExpressionAttributeValue(alias string, resolved KeyValue) *ExpressionAttributeValue {
	return &ExpressionAttributeValue{Alias: alias, Resolved: resolved}
}

// note: There is no "raw value" node in the key condition parse tree afaik,
// only expression attribute values are allowed, because otherwise we would need type inference
type ExpressionAttributeValue struct {
	Alias    string
	Resolved KeyValue
}

func (v *ExpressionAttributeValue) GetValue() KeyValue {
	return v.Resolved
}
