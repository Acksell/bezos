package ddbsdk

import (
	"bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// Put without optimistic locking
func NewUnsafePut(table table.TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return newPut(table, key, e)
}

type VersionedDynamoEntity interface {
	DynamoEntity
	// Version should return the dynamodb field name and current value of the version field.
	Version() (string, any)
}

// Put with optimistic locking
func NewSafePut(table table.TableDefinition, key table.PrimaryKey, e VersionedDynamoEntity) *Put {
	versionField, version := e.Version()
	return newPut(table, key, e).WithCondition(
		expression.Equal(expression.Name(versionField), expression.Value(version)).
			Or(expression.AttributeNotExists(expression.Name(versionField))))
}
