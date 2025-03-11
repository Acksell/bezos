package bzoddb

import "bezos"

// todo is this the spec or the runtime interface?
type DynamoEntity interface {
	bezos.Entity
	IsValid() error

	// Lock() LockingStrategy

	// Schema in order to validate that the field-specific operations are valid.
	// Schema() EntitySchema
}
