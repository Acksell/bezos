package normddb

import (
	"norm/normddb/table"
	"time"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type Action interface {
	TableName() *string
	PrimaryKey() table.PrimaryKey
}

type Put struct {
	Table TableDefinition
	// Index  Index
	Entity DynamoEntity
	Key    table.PrimaryKey

	ttlExpiry *time.Time

	c expression2.ConditionBuilder
}

// UnsafeUpdate is called unsafe because it does not require the user to
// check the invariants of the entity they're modifying. The safety of the
// operation relies solely on the user doing careful validations before committing.
type UnsafeUpdate struct {
	Table  TableDefinition
	Key    table.PrimaryKey
	Fields map[string]UpdateOp

	ttlExpiry          *time.Time
	allowNonIdempotent bool

	u expression2.UpdateBuilder
	c expression2.ConditionBuilder
}

type Delete struct {
	Table TableDefinition
	Key   table.PrimaryKey

	c expression2.ConditionBuilder
}
