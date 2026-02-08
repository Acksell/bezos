package ddbsdk

import (
	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// SortKeyStrategy defines how to filter on the sort key in a range query.
type SortKeyStrategy func(skName string) expression2.KeyConditionBuilder

// Equals returns items where the sort key equals the provided value.
func Equals[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyEqual(expression2.Key(skName), expression2.Value(v))
	}
}

// BeginsWith returns items where the sort key starts with the provided prefix.
func BeginsWith(prefix string) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyBeginsWith(expression2.Key(skName), prefix)
	}
}

// Between returns items where the sort key is between start and end (inclusive).
func Between[T any](start, end T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyBetween(
			expression2.Key(skName),
			expression2.Value(start),
			expression2.Value(end),
		)
	}
}

// GreaterThan returns items where the sort key is greater than the provided value.
func GreaterThan[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyGreaterThan(expression2.Key(skName), expression2.Value(v))
	}
}

// GreaterThanOrEqual returns items where the sort key is greater than or equal to the provided value.
func GreaterThanOrEqual[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyGreaterThanEqual(expression2.Key(skName), expression2.Value(v))
	}
}

// LessThan returns items where the sort key is less than the provided value.
func LessThan[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyLessThan(expression2.Key(skName), expression2.Value(v))
	}
}

// LessThanOrEqual returns items where the sort key is less than or equal to the provided value.
func LessThanOrEqual[T any](v T) SortKeyStrategy {
	return func(skName string) expression2.KeyConditionBuilder {
		return expression2.KeyLessThanEqual(expression2.Key(skName), expression2.Value(v))
	}
}

func ptr[T any](v T) *T {
	return &v
}
