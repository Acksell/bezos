package index

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

// PrimaryIndex represents the main table index with its key formats and associated GSIs.
// The type parameter E is the entity type, used by code generators for type inference.
//
// Example:
//
//	var UsersIndex = index.PrimaryIndex[User]{
//	    Table:        UsersTable,
//	    PartitionKey: val.Fmt("USER#{id}"),
//	    SortKey:      val.Fmt("PROFILE").Ptr(),
//	}
type PrimaryIndex[E any] struct {
	// Table is the underlying table definition (contains key definitions)
	Table table.TableDefinition `json:"table"`
	// PartitionKey is the value definition for the partition key
	PartitionKey val.ValDef `json:"partitionKey"`
	// SortKey is the value definition for the sort key (nil if no sort key)
	SortKey *val.ValDef `json:"sortKey,omitempty"`
	// Secondary are the Global Secondary Indexes associated with this table
	Secondary []SecondaryIndex `json:"secondary,omitempty"`
}

// TableName returns the table name.
func (pi *PrimaryIndex[E]) TableName() string {
	return pi.Table.Name
}

// Validate checks that the PrimaryIndex is properly configured.
func (pi *PrimaryIndex[E]) Validate() error {
	if pi.Table.Name == "" {
		return fmt.Errorf("table name is required")
	}
	if pi.PartitionKey.IsZero() {
		return fmt.Errorf("partition key format is required")
	}

	for _, gsi := range pi.Secondary {
		if err := gsi.Validate(); err != nil {
			return fmt.Errorf("GSI %q: %w", gsi.Name(), err)
		}
	}

	return nil
}
