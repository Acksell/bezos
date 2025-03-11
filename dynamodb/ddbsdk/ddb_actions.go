package bzoddb

import (
	"bezos/dynamodb/table"
	"time"

	expression2 "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Action interface {
	TableName() *string
	PrimaryKey() (table.PrimaryKey, error)
}

type Put struct {
	Index  table.PrimaryIndexDefinition
	Entity DynamoEntity

	ttlExpiry *time.Time

	c expression2.ConditionBuilder

	doc map[string]types.AttributeValue
}

// UnsafeUpdate is called unsafe because it does not require the user to
// check the invariants of the entity they're modifying. The safety of the
// operation relies solely on the user doing careful validations before committing.
// Furthermore there may be unintended race conditions from concurrent modifications
// unless using optimistic locking (by using WithCondition).
type UnsafeUpdate struct {
	Table  table.TableDefinition
	Key    table.PrimaryKey // todo how does the user construct this key using indices?
	Fields map[string]UpdateOp

	ttlExpiry          *time.Time
	allowNonIdempotent bool

	u expression2.UpdateBuilder
	c expression2.ConditionBuilder
}

type Delete struct {
	Table table.TableDefinition
	Key   table.PrimaryKey

	c expression2.ConditionBuilder
}
