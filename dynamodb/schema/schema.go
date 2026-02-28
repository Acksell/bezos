// Package schema defines the data types for DynamoDB table and entity schemas.
// These types are used by ddbgen for code generation output and by ddbui for
// runtime schema introspection. The types are pure data structures with no methods.
package schema

// Schema is the root type containing all table definitions.
// This maps directly to the structure of schema_dynamodb.yaml files.
type Schema struct {
	Tables []Table `yaml:"tables" json:"tables"`
}

// Table describes a DynamoDB table structure with its entities.
type Table struct {
	Name         string   `yaml:"name" json:"name"`
	PartitionKey KeyDef   `yaml:"partitionKey" json:"partitionKey"`
	SortKey      *KeyDef  `yaml:"sortKey,omitempty" json:"sortKey,omitempty"`
	GSIs         []GSI    `yaml:"gsis,omitempty" json:"gsis,omitempty"`
	Entities     []Entity `yaml:"entities,omitempty" json:"entities,omitempty"`
}

// KeyDef describes a key attribute definition.
type KeyDef struct {
	Name string `yaml:"name" json:"name"`
	Kind string `yaml:"kind" json:"kind"` // "S", "N", or "B"
}

// GSI describes a Global Secondary Index.
type GSI struct {
	Name         string  `yaml:"name" json:"name"`
	PartitionKey KeyDef  `yaml:"partitionKey" json:"partitionKey"`
	SortKey      *KeyDef `yaml:"sortKey,omitempty" json:"sortKey,omitempty"`
}

// Entity describes an entity type stored in a table.
type Entity struct {
	Type                string       `yaml:"type" json:"type"`
	PartitionKeyPattern string       `yaml:"partitionKeyPattern" json:"partitionKeyPattern"`
	SortKeyPattern      string       `yaml:"sortKeyPattern,omitempty" json:"sortKeyPattern,omitempty"`
	Fields              []Field      `yaml:"fields" json:"fields"`
	GSIMappings         []GSIMapping `yaml:"gsiMappings,omitempty" json:"gsiMappings,omitempty"`
	IsVersioned         bool         `yaml:"isVersioned,omitempty" json:"isVersioned,omitempty"`
}

// Field describes an entity field.
type Field struct {
	Name string `yaml:"name" json:"name"`
	Tag  string `yaml:"tag" json:"tag"`
	Type string `yaml:"type" json:"type"`
}

// GSIMapping describes how an entity maps to a GSI.
type GSIMapping struct {
	GSI              string `yaml:"gsi" json:"gsi"`
	PartitionPattern string `yaml:"partitionPattern" json:"partitionPattern"`
	SortPattern      string `yaml:"sortPattern,omitempty" json:"sortPattern,omitempty"`
}
