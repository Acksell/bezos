package index

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/table"
)

// SecondaryIndex represents a Global Secondary Index (GSI) definition with
// key patterns for extracting GSI key values from items.
//
// Example:
//
//	index.SecondaryIndex{
//	    Name: "ByEmail",
//	    Partition: index.KeyValDef{
//	        KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
//	        ValDef: keys.Fmt("EMAIL#{email}"),
//	    },
//	    Sort: &index.KeyValDef{
//	        KeyDef: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
//	        ValDef: keys.Fmt("USER#{id}"),
//	    },
//	}
type SecondaryIndex struct {
	// Name is the GSI name as defined in DynamoDB
	Name string
	// Partition defines the GSI partition key
	Partition KeyValDef
	// Sort defines the GSI sort key (optional)
	Sort *KeyValDef
}

// KeyDefinition returns the key definition for this GSI.
func (si SecondaryIndex) KeyDefinition() table.PrimaryKeyDefinition {
	keyDef := table.PrimaryKeyDefinition{
		PartitionKey: si.Partition.KeyDef,
	}
	if si.Sort != nil {
		keyDef.SortKey = si.Sort.KeyDef
	}
	return keyDef
}

// Validate checks that the SecondaryIndex is properly configured.
func (si SecondaryIndex) Validate() error {
	if si.Name == "" {
		return fmt.Errorf("secondary index name is required")
	}
	if si.Partition.KeyDef.Name == "" {
		return fmt.Errorf("partition key name is required for GSI %q", si.Name)
	}
	if !si.Partition.ValDef.HasValueSource() {
		return fmt.Errorf("partition key value source (Fmt, FromField, or Const) is required for GSI %q", si.Name)
	}
	return nil
}
