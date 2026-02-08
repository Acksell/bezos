package index

import (
	"fmt"

	"github.com/acksell/bezos/dynamodb/keys"
	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// SecondaryIndex represents a Global Secondary Index (GSI) definition with
// key formats for extracting GSI key values from items.
//
// Example:
//
//	var EmailGSI = indices.SecondaryIndex{
//	    Name:         "gsi1",
//	    PartitionKey: keys.Key{Def: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS}, Extractor: keys.Fmt("EMAIL#%s", keys.Field("email"))},
//	    SortKey:      &keys.Key{Def: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS}, Extractor: keys.Fmt("USER#%s", keys.Field("userID"))},
//	}
type SecondaryIndex struct {
	// Name is the GSI name as defined in DynamoDB
	Name string
	// PartitionKey defines the GSI partition key
	PartitionKey keys.Key
	// SortKey defines the GSI sort key (optional)
	SortKey *keys.Key
}

// ExtractKeys extracts the GSI key attributes from the item.
// Returns a map with the GSI partition key and sort key (if defined) attributes.
// Returns nil (not an error) if required fields are missing - DynamoDB handles sparse GSIs.
func (si SecondaryIndex) ExtractKeys(item map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue, 2)

	pkVal, err := si.PartitionKey.FromItem(item)
	if err != nil {
		// Field not present - this item won't be indexed (sparse GSI behavior)
		return nil, nil
	}
	result[si.PartitionKey.Def.Name] = pkVal

	if si.SortKey != nil {
		skVal, err := si.SortKey.FromItem(item)
		if err != nil {
			// Sort key field not present - don't index
			return nil, nil
		}
		result[si.SortKey.Def.Name] = skVal
	}

	return result, nil
}

// ToTableDefinition returns a table.TableDefinition representing this GSI.
// This is for compatibility with existing code that treats GSIs as tables.
func (si SecondaryIndex) ToTableDefinition(parentTable table.TableDefinition) table.TableDefinition {
	keyDef := table.PrimaryKeyDefinition{
		PartitionKey: si.PartitionKey.Def,
	}
	if si.SortKey != nil {
		keyDef.SortKey = si.SortKey.Def
	}

	return table.TableDefinition{
		Name:           parentTable.Name,
		KeyDefinitions: keyDef,
		TimeToLiveKey:  parentTable.TimeToLiveKey,
		IsGSI:          true,
	}
}

// Validate checks that the SecondaryIndex is properly configured.
func (si SecondaryIndex) Validate() error {
	if si.Name == "" {
		return fmt.Errorf("secondary index name is required")
	}
	if si.PartitionKey.Def.Name == "" {
		return fmt.Errorf("partition key name is required for GSI %q", si.Name)
	}
	if si.PartitionKey.Extractor == nil {
		return fmt.Errorf("partition key extractor is required for GSI %q", si.Name)
	}
	return nil
}
