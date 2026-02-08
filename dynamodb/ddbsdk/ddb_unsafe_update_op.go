package ddbsdk

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"golang.org/x/exp/constraints"
)

type UpdateOp interface {
	Field() string
	Apply(expression.UpdateBuilder) expression.UpdateBuilder
	IsIdempotent() bool
}
type number interface {
	constraints.Integer | constraints.Float
}

// type dynamoBaseValueType interface {
// 	number | ~string | ~[]byte
// }

// Sets the value of a field regardless of any existing value
type setFieldOp[T any] struct {
	field string
	value T
}

var _ UpdateOp = &setFieldOp[string]{}

func SetFieldOp[T any](field string, value T) setFieldOp[T] {
	return setFieldOp[T]{
		field: field,
		value: value,
	}
}

func (o setFieldOp[T]) Field() string {
	return o.field
}

func (o setFieldOp[T]) IsIdempotent() bool {
	return true
}

func (o setFieldOp[T]) Apply(expr expression.UpdateBuilder) expression.UpdateBuilder {
	return expr.Add(expression.Name(o.field), expression.Value(o.value))
}

type removeFieldOp struct {
	field string
}

func RemoveFieldOp(field string) removeFieldOp {
	return removeFieldOp{
		field: field,
	}
}

func (o removeFieldOp) IsIdempotent() bool {
	return true
}
func (o removeFieldOp) Field() string {
	return o.field
}

func (o removeFieldOp) Apply(expr expression.UpdateBuilder) expression.UpdateBuilder {
	return expr.Remove(expression.Name(o.field))
}

type subtractFromSetOp[T any] struct {
	field string
	value T
}

func SubtractFromSetOp[T any](field string, value T) subtractFromSetOp[T] {
	return subtractFromSetOp[T]{
		field: field,
		value: value,
	}
}

func (subtractFromSetOp[T]) IsIdempotent() bool {
	return true
}

func (o subtractFromSetOp[T]) Apply(expr expression.UpdateBuilder) expression.UpdateBuilder {
	return expr.Delete(expression.Name(o.field), expression.Value(o.value))
}

type appendToListOp[T any] struct {
	field string
	value T
}

func AppendToListOp[T any](field string, value T) appendToListOp[T] {
	return appendToListOp[T]{
		field: field,
		value: value,
	}
}

func (appendToListOp[T]) IsIdempotent() bool {
	return false
}

func (o appendToListOp[T]) Field() string {
	return o.field
}

func (o appendToListOp[T]) Apply(expr expression.UpdateBuilder) expression.UpdateBuilder {
	return expr.Add(expression.Name(o.field), expression.Value(o.value))
}

type addNumberOp[T number] struct {
	field string
	value T
}

func AddNumberOp[T number](field string, value T) addNumberOp[T] {
	return addNumberOp[T]{
		field: field,
		value: value,
	}
}

func (addNumberOp[T]) IsIdempotent() bool {
	return false
}

func (o addNumberOp[T]) Field() string {
	return o.field
}

func (o addNumberOp[T]) Apply(expr expression.UpdateBuilder) expression.UpdateBuilder {
	return expr.Add(expression.Name(o.field), expression.Value(o.value))
}

//todo can type this more properly, and autogenerate the field variables so you don't rely on hardcoded strings.
// type Field struct {
// 	Name string
// }
