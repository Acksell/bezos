package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	TimeToLiveKey  string
	IsGSI          bool
	// todo make interface instead? "IsGSI" is begging for inconsistencies, needs type safety instead, whilst still being able to treat GSIs as tables in code.
	GSIs []TableDefinition
}

// todo not sure if this should be in this package. I don't see the immediate benefit. The use case is mostly internal.
func (t TableDefinition) ExtractPrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	part, ok := doc[t.KeyDefinitions.PartitionKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("partition key not found")
	}
	if err := attributeMatchesDefinition(t.KeyDefinitions.PartitionKey.Kind, part); err != nil {
		return PrimaryKey{}, fmt.Errorf("partition key kind does not match definition")
	}
	pk := PrimaryKey{
		Definition: t.KeyDefinitions,
		Values: PrimaryKeyValues{
			PartitionKey: keyValueFromAV(part),
		},
	}
	if t.KeyDefinitions.SortKey.Name == "" {
		return pk, nil
	}
	sort, ok := doc[t.KeyDefinitions.SortKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("sort key not found")
	}
	if err := attributeMatchesDefinition(t.KeyDefinitions.SortKey.Kind, sort); err != nil {
		return PrimaryKey{}, fmt.Errorf("sort key kind does not match definition")
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
