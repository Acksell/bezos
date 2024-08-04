package normddb

import "time"

type IndexSpec struct {
	Name            string
	UnderlyingTable TableDefinition
	// All entities put into this table get this TTL unless overridden by the Put operation.
	// Zero duration means indefinite storage
	DefaultTTL time.Duration

	Projections []ProjectionSpec
	// Queries
	// NewQuery(DynamoEntity) *Query
}

// Projections are attached to an index where the source data is stored.
// Initially we only implement eventually consistent projections, aka GSIs.
// But some services use "consistent projections", this will be added later.
type ProjectionSpec struct {
	Name string
	// Used for knowing the GSI's, and also for temporal logic checks warning of potential race conditions.
	// When true, the index
	EventuallyConsistent bool

	// KeyNames defines the primary key of the new document stored.
	KeyDefinitions PrimaryKeyDefinition

	// todo: implement
	Strategy ProjectionStrategy

	Supports []QueryPattern
}

type ProjectionStrategy string

const (
	INCLUDE_KEYS_ONLY ProjectionStrategy = "KEYS_ONLY"
	INCLUDE_SOME      ProjectionStrategy = "INCLUDE"
	INCLUDE_ALL       ProjectionStrategy = "ALL"
)
