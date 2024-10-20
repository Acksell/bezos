package normddb

import (
	"norm/normddb/table"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// Put without optimistic locking
func NewUnsafePut(table table.TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e)
}

// Put with optimistic locking
func NewSafePut(table table.TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e).WithCondition(
		expression.Equal(expression.Name("meta.updated"), expression.Value(e.GetMeta().Updated.Format(time.RFC3339Nano))))
}
