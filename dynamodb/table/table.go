package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	TimeToLiveKey  string
	GSIs           []GSIDefinition
}

// GSIDefinition represents a Global Secondary Index definition.
type GSIDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
}

// ExtractPrimaryKey extracts the primary key values from a document.
func (g GSIDefinition) ExtractPrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	return g.KeyDefinitions.ExtractPrimaryKey(doc)
}

func (t TableDefinition) ExtractPrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	return t.KeyDefinitions.ExtractPrimaryKey(doc)
}

func (k PrimaryKeyDefinition) ExtractPrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	part, ok := doc[k.PartitionKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("partition key %q not found", k.PartitionKey.Name)
	}
	if err := attributeMatchesDefinition(k.PartitionKey.Kind, part); err != nil {
		return PrimaryKey{}, fmt.Errorf("document key %q kind does not match definition: %w", k.PartitionKey.Name, err)
	}
	pk := PrimaryKey{
		Definition: k,
		Values: PrimaryKeyValues{
			PartitionKey: keyValueFromAV(part),
		},
	}
	if k.SortKey.Name == "" {
		return pk, nil
	}
	sort, ok := doc[k.SortKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("sort key %q not found on document", k.SortKey.Name)
	}
	if err := attributeMatchesDefinition(k.SortKey.Kind, sort); err != nil {
		return PrimaryKey{}, fmt.Errorf("sort key %q kind does not match definition: %w", k.SortKey.Name, err)
	}
	pk.Values.SortKey = keyValueFromAV(sort)
	return pk, nil
}

func keyValueFromAV(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	default:
		panic(fmt.Sprintf("unsupported attribute value %T for dynamodb keys", v))
	}
}
