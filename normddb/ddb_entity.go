package normddb

import "norm"

// todo is this the spec or the runtime interface?
type DynamoEntity interface {
	norm.Entity
	SchemaName() string

	// PrimaryKey() PrimaryKey // there is no common index for all entities, except maybe the direct lookup by ID index?
	// GSIKeys() []PrimaryKey

	// DefaultTTL() time.Duration

	IsValid() error
	// Lock() LockingStrategy

	// Schema in order to validate that the field-specific operations are valid.
	// Schema() EntitySchema
}