package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Primary indexes operates on the underlying table's keys.
// The index definition contains "Keyers" which construct the primary key.
type PrimaryIndexDefinition struct {
	Table          TableDefinition
	PartitionKeyer Keyer
	SortKeyer      Keyer
}

func (i *PrimaryIndexDefinition) PrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	part, err := i.PartitionKeyer.Key(doc)
	if err != nil {
		return PrimaryKey{}, fmt.Errorf("failed to get partition key: %w", err)
	}
	pk := PrimaryKey{
		Definition: i.Table.KeyDefinitions,
		Values: PrimaryKeyValues{
			PartitionKey: keyValueFromAV(part),
		},
	}
	if i.Table.KeyDefinitions.SortKey.Name == "" {
		return pk, nil
	}
	sort, err := i.SortKeyer.Key(doc)
	if err != nil {
		return PrimaryKey{}, fmt.Errorf("failed to get sort key: %w", err)
	}
	pk.Values.SortKey = keyValueFromAV(sort)
	if err := attributeMatchesDefinition(i.Table.KeyDefinitions.PartitionKey.Kind, part); err != nil {
		return PrimaryKey{}, fmt.Errorf("partition key kind does not match table definition: %w", err)
	}
	if err := attributeMatchesDefinition(i.Table.KeyDefinitions.SortKey.Kind, sort); err != nil {
		return PrimaryKey{}, fmt.Errorf("sort key kind does not match table definition: %w", err)
	}
	return pk, nil
}
