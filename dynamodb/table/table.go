package table

type TableDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	TimeToLiveKey  string
	Projections    []SecondaryIndexDefinition

	// Optional field for registering entity schemas.
	// Allows for validation of database operations.
	// Entities map[string]EntitySchema
}
