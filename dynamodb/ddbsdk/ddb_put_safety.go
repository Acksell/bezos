package ddbsdk

import (
	"github.com/acksell/bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// Put without optimistic locking
func NewUnsafePut(table table.TableDefinition, key table.PrimaryKey, e DynamoEntity) *Put {
	return newPut(table, key, e)
}

type VersionedDynamoEntity interface {
	DynamoEntity
	// VersionField should return the dynamodb field name and current value of the version field.
	VersionField() (string, any)
}

// Put with optimistic locking.
//
// nil `old` => assert PK doesn't exist.
// non-nil `old` => assert version match.
func NewSafePut[E VersionedDynamoEntity](table table.TableDefinition, key table.PrimaryKey, old E, new E) *PutWithCondition {
	var zero E
	if any(old) == any(zero) {
		// Conditional create: item must not exist yet
		return newPut(table, key, new).WithCondition(
			expression.AttributeNotExists(expression.Name(table.KeyDefinitions.PartitionKey.Name)))
	}
	// Optimistic locking: existing version must equal old's version
	versionField, oldVersion := old.VersionField()
	return newPut(table, key, new).WithCondition(
		expression.Equal(expression.Name(versionField), expression.Value(oldVersion)))
}
