package normddb

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func NewCreate(table TableDescription, key PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e).WithCondition(
		expression.AttributeNotExists(expression.Name("meta.created")))
}
