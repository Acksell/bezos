package normddb

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func NewUnsafePut(table TableDescription, key PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e)
}

func NewSafePut(table TableDescription, key PrimaryKey, e DynamoEntity) *Put {
	return NewPut(table, key, e).WithCondition(
		expression.Equal(expression.Name("meta.updated"), expression.Value(e.GetMeta().Updated.Format(time.RFC3339Nano))))
}
