package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Secondary indices define extra keys to be populated on a document.
// // Secondary indices can not be directly written to, they should be updated by
// // writes to a primary index. It is a projection of the original table.
// // Secondary indices define their own keys, separate from the primary index.
// // The index definition contains "Keyers" which construct the primary key.

// // If IsConsistent is false, the secondary is a simple GSI.
// // If IsConsistent is true, the secondary is a strongly consistent GSI.
// // The rules for deciding whether a document goes into secondary index are the same as for GSI's:
// // - If the partition key is non-nil, the document goes into the index.
// // - Changes to the sort key or partition key results in a transactional delete+put.
// // - Removal of the original document results in a delete.
type SecondaryIndexDefinition struct {
	Name           string
	KeyDefinitions PrimaryKeyDefinition
	PartitionKeyer Keyer
	SortKeyer      Keyer

	// // Defaults to the same table as the original document.
	// // This option is only needed for strongly consistent secondary indexes.
	// StorageTable *TableDefinition

	// ProjectionStrategy ProjectionDefinition
	// // A consistent SecondaryIndexDefinition can be used to do projections in a consistent manner,
	// // effectively replicating a GSI but with strong consistency. This is done by adding new writes
	// // to the the SecondaryIndex's key's.
	// IsConsistent bool
}

func (i *SecondaryIndexDefinition) PrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	part, err := i.PartitionKeyer.Key(doc)
	if err != nil {
		return PrimaryKey{}, fmt.Errorf("failed to get partition key: %w", err)
	}
	pk := PrimaryKey{
		Definition: i.KeyDefinitions,
		Values: PrimaryKeyValues{
			PartitionKey: keyValueFromAV(part),
		},
	}
	if i.KeyDefinitions.SortKey.Name == "" {
		return pk, nil
	}
	sort, err := i.SortKeyer.Key(doc)
	if err != nil {
		return PrimaryKey{}, fmt.Errorf("failed to get sort key: %w", err)
	}
	pk.Values.SortKey = keyValueFromAV(sort)
	if err := attributeMatchesDefinition(i.KeyDefinitions.PartitionKey.Kind, part); err != nil {
		return PrimaryKey{}, fmt.Errorf("partition key kind does not match table definition: %w", err)
	}
	if err := attributeMatchesDefinition(i.KeyDefinitions.SortKey.Kind, sort); err != nil {
		return PrimaryKey{}, fmt.Errorf("sort key kind does not match table definition: %w", err)
	}
	return pk, nil
}

// func (i *SecondaryIndexDefinition) Project(doc map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
// 	part, err := i.PartitionKeyer.Key(doc)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get partition key: %w", err)
// 	}
// 	proj := map[string]types.AttributeValue{
// 		i.KeyDefinitions.PartitionKey.Name: part,
// 	}
// 	if i.KeyDefinitions.SortKey.Name == "" {
// 		return proj, nil
// 	}
// 	sort, err := i.SortKeyer.Key(doc)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get sort key: %w", err)
// 	}
// 	proj[i.KeyDefinitions.SortKey.Name] = sort
// 	return proj, nil
// }

// type ProjectionDefinition struct {
// 	Kind ProjectionKind
// 	// In addition to the attributes described in KEYS_ONLY, the secondary index will include other non-key attributes that you specify.
// 	// Will only be used if the ProjectionKind is ProjectSubset.
// 	NonKeyAttributes []types.AttributeValue
// }

// type ProjectionKind string

// const (
// 	ProjectAll      ProjectionKind = "ALL"
// 	ProjectOnlyKeys ProjectionKind = "KEYS_ONLY"
// 	ProjectSubset   ProjectionKind = "INCLUDE"
// )

// type Projection interface {
// 	Project(doc map[string]types.AttributeValue) map[string]types.AttributeValue
// }

// var projectAll Projection = allProjection{}

// type allProjection struct{}

// func (p allProjection) Project(doc map[string]types.AttributeValue) map[string]types.AttributeValue {
// 	return doc
// }

// func newSubsetProjection(attrs ...string) Projection {
// 	return subsetProjection{attrs}
// }

// type subsetProjection struct {
// 	attrs []string
// }

// func (p subsetProjection) Project(doc map[string]types.AttributeValue) map[string]types.AttributeValue {
// 	proj := make(map[string]types.AttributeValue)
// 	for _, key := range p.attrs {
// 		if val, ok := doc[key]; ok {
// 			proj[key] = val
// 		}
// 	}
// 	return proj
// }
