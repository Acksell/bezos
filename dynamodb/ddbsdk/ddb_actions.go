package ddbsdk

import (
	"time"

	"github.com/acksell/bezos/dynamodb/table"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type Action interface {
	TableName() *string
	PrimaryKey() table.PrimaryKey

	txWriteItem
}

// Put
var _ Action = &Put{}
var _ Action = &PutWithCondition{}

// Delete
var _ Action = &Delete{}
var _ Action = &DeleteWithCondition{}

// Update
var _ Action = &UnsafeUpdate{}

// BatchAction represents actions that can be used with BatchWriteItem.
// Only Put and Delete are supported (not UnsafeUpdate or PutWithCondition).
// The private batchWritable() method restricts implementations to this package.
type BatchAction interface {
	Action
	batchWritable() // marker method to restrict implementations
}

// Put represents a Put operation that can be batch-written.
// Use WithCondition to add a condition, which converts it to PutWithCondition.
type Put struct {
	Table table.TableDefinition
	// Index  Index
	Entity DynamoEntity
	Key    table.PrimaryKey

	ttlExpiry *time.Time

	c expression2.ConditionBuilder
}

// PutWithCondition wraps Put with a condition expression.
// It cannot be used with BatchWriteItem (use TransactWriteItems or PutItem instead).
type PutWithCondition struct {
	put *Put
}

type Delete struct {
	Table table.TableDefinition
	Key   table.PrimaryKey

	c expression2.ConditionBuilder
}

// DeleteWithCondition wraps Delete with a condition expression.
// It cannot be used with BatchWriteItem (use TransactWriteItems or DeleteItem instead).
type DeleteWithCondition struct {
	del *Delete
}

// UnsafeUpdate is called unsafe because it does not require the user to
// check the invariants of the entity they're modifying. The safety of the
// operation relies solely on the user doing careful validations before committing.
type UnsafeUpdate struct {
	Table  table.TableDefinition
	Key    table.PrimaryKey
	Fields map[string]UpdateOp

	ttlExpiry          *time.Time
	allowNonIdempotent bool

	u expression2.UpdateBuilder
	c expression2.ConditionBuilder
}
