package bzoddb

import (
	"bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// Put without optimistic locking
// It is recommended to use NewSafePut instead of NewUnsafePut.
func NewUnsafePut(index table.PrimaryIndexDefinition, e DynamoEntity) *Put {
	return newPut(index, e)
}

type VersionedDynamoEntity interface {
	DynamoEntity
	// Version should return the dynamodb field name and current value of the version field
	Version() (string, any)
}

// Put with optimistic locking.
// Aborts the transaction if the version was changed by another transaction before committing.
func NewSafePut(index table.PrimaryIndexDefinition, e VersionedDynamoEntity) *Put {
	versionField, version := e.Version()
	return newPut(index, e).WithCondition(
		expression.Equal(expression.Name(versionField), expression.Value(version)).
			Or(expression.AttributeNotExists(expression.Name(versionField))))
}
