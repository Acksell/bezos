package normddb

import (
	"norm/normddb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func NewCreate(table TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e).WithCondition(
		expression.AttributeNotExists(expression.Name("meta.created")))
}
