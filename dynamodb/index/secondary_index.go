package index

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

// SecondaryIndex represents a Global Secondary Index (GSI) definition with
// key value patterns for extracting GSI key values from items.
//
// Example:
//
//	index.SecondaryIndex{
//	    GSI:       UserTable.GSIs[0],
//	    Partition: val.Fmt("EMAIL#{email}"),
//	    Sort:      val.Fmt("USER#{id}").Ptr(),
//	}
type SecondaryIndex struct {
	// GSI is the GSI definition from the table (contains Name and KeyDefinitions)
	GSI table.GSIDefinition
	// Partition defines how to derive the GSI partition key value
	Partition val.ValDef
	// Sort defines how to derive the GSI sort key value (optional)
	Sort *val.ValDef
}

// Name returns the GSI name.
func (si SecondaryIndex) Name() string {
	return si.GSI.Name
}

// KeyDefinition returns the key definition for this GSI.
func (si SecondaryIndex) KeyDefinition() table.PrimaryKeyDefinition {
	return si.GSI.KeyDefinitions
}

// Validate checks that the SecondaryIndex is properly configured.
func (si SecondaryIndex) Validate() error {
	if si.GSI.Name == "" {
		return fmt.Errorf("secondary index GSI name is required")
	}
	if si.GSI.KeyDefinitions.PartitionKey.Name == "" {
		return fmt.Errorf("partition key name is required for GSI %q", si.GSI.Name)
	}
	if !si.Partition.HasValueSource() {
		return fmt.Errorf("partition key value source (Fmt, FromField, or Const) is required for GSI %q", si.GSI.Name)
	}
	return nil
}
