package table

type TableDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	TimeToLiveKey  string
	IsGSI          bool
	GSIs           []GSIDefinition
	// GlobalProjections []ProjectionSpec

	// Optional field for registering entity schemas.
	// Allows for validation of database operations.
	// Entities map[string]EntitySchema

	// Indicies []Index
}

type GSIDefinition struct {
	IndexName string
	Key       PrimaryKeyDefinition

	// Entities []DynamoEntity // do reflection and check if the entity implements the key
}
