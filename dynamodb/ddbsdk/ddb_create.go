package bzoddb

import (
	"bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func NewCreate(index table.PrimaryIndexDefinition, e DynamoEntity) *Put {
	return newPut(index, e).WithCondition(
		expression.AttributeNotExists(expression.Name("meta.created")))
}
