package index

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/keys"
	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// PrimaryIndex represents the main table index with its key formats and associated GSIs.
// It serves as the central definition for a DynamoDB table's access patterns.
//
// Example:
//
//	var UsersIndex = indices.PrimaryIndex{
//	    Table:        UsersTable,
//	    PartitionKey: keys.Fmt("USER#%s", keys.Field("userID")),
//	    SortKey:      keys.Const("PROFILE"),
//	    Secondary:    []indices.SecondaryIndex{EmailGSI, StatusGSI},
//	}
type PrimaryIndex struct {
	// Table is the underlying table definition (contains key definitions)
	Table table.TableDefinition
	// PartitionKey extracts the partition key value
	PartitionKey keys.Extractor
	// SortKey extracts the sort key value (optional - use nil if not needed)
	SortKey keys.Extractor
	// Secondary are the Global Secondary Indexes associated with this table
	Secondary []SecondaryIndex
}

// TableName returns the table name.
func (pi PrimaryIndex) TableName() string {
	return pi.Table.Name
}

// ExtractPrimaryKey extracts the primary key values from the item.
func (pi PrimaryIndex) ExtractPrimaryKey(item map[string]types.AttributeValue) (table.PrimaryKey, error) {
	pkVal, err := pi.PartitionKey.Extract(item)
	if err != nil {
		return table.PrimaryKey{}, fmt.Errorf("extract partition key: %w", err)
	}

	pk := table.PrimaryKey{
		Definition: pi.Table.KeyDefinitions,
		Values: table.PrimaryKeyValues{
			PartitionKey: fmt.Sprint(pkVal),
		},
	}

	skDef := pi.Table.KeyDefinitions.SortKey
	if skDef.Name != "" && pi.SortKey != nil {
		skVal, err := pi.SortKey.Extract(item)
		if err != nil {
			return table.PrimaryKey{}, fmt.Errorf("extract sort key: %w", err)
		}
		pk.Values.SortKey = fmt.Sprint(skVal)
	}

	return pk, nil
}

// ExtractAllGSIKeys extracts GSI key attributes for all GSIs from the item.
// Returns a merged map of all GSI key attributes that should be added to the item.
// GSIs with missing fields are silently skipped (sparse GSI behavior).
func (pi PrimaryIndex) ExtractAllGSIKeys(item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue)

	for _, gsi := range pi.Secondary {
		gsiKeys, err := gsi.ExtractKeys(item)
		if err != nil {
			return nil, fmt.Errorf("GSI %q: %w", gsi.Name, err)
		}
		// gsiKeys is nil if fields are missing (sparse GSI) - skip
		if gsiKeys == nil {
			continue
		}
		for k, v := range gsiKeys {
			result[k] = v
		}
	}

	return result, nil
}

// Validate checks that the PrimaryIndex is properly configured.
func (pi PrimaryIndex) Validate() error {
	if pi.Table.Name == "" {
		return fmt.Errorf("table name is required")
	}
	if pi.PartitionKey == nil {
		return fmt.Errorf("partition key extractor is required")
	}

	for _, gsi := range pi.Secondary {
		if err := gsi.Validate(); err != nil {
			return fmt.Errorf("GSI %q: %w", gsi.Name, err)
		}
	}

	return nil
}
