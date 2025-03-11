package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PrimaryKeyDefinition struct {
	PartitionKey KeyDef
	SortKey      KeyDef
}

type KeyDef struct {
	Name string
	Kind KeyKind
}

type KeyKind string

const (
	KeyKindS KeyKind = "S"
	KeyKindN KeyKind = "N"
	KeyKindB KeyKind = "B"
)

// Type safety can be ensured by using type constrained constructors generated based on the Table's KeyDefinition.
type PrimaryKeyValues struct {
	PartitionKey any
	SortKey      any
}

type PrimaryKey struct {
	Definition PrimaryKeyDefinition
	Values     PrimaryKeyValues
}

// TODO return error instead
func (k PrimaryKey) DDB() map[string]types.AttributeValue {
	pk, err := attributevalue.Marshal(k.Values.PartitionKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal partition key of type %T with value %v: %w", k.Values.PartitionKey, k.Values.PartitionKey, err))
	}
	err = attributeMatchesDefinition(k.Definition.PartitionKey.Kind, pk)
	if err != nil {
		panic(fmt.Errorf("key kind does not match dynamo value: %w", err))
	}

	sk, err := attributevalue.Marshal(k.Values.SortKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal sort key of type %T with value %v: %w", k.Values.SortKey, k.Values.SortKey, err))
	}
	err = attributeMatchesDefinition(k.Definition.SortKey.Kind, sk)
	if err != nil {
		panic(fmt.Errorf("key kind does not match dynamo value: %w", err))
	}

	return map[string]types.AttributeValue{
		k.Definition.PartitionKey.Name: pk,
		k.Definition.SortKey.Name:      sk,
	}
}

func attributeMatchesDefinition(want KeyKind, v types.AttributeValue) error {
	var got KeyKind
	switch v.(type) {
	case *types.AttributeValueMemberS:
		got = KeyKindS
	case *types.AttributeValueMemberN:
		got = KeyKindN
	case *types.AttributeValueMemberB:
		got = KeyKindB
	default:
		return fmt.Errorf("unexpected key attribute type %T", v)
	}
	if got != want {
		return fmt.Errorf("got KeyKind %q want %q", got, want)
	}
	return nil
}

func (def PrimaryKeyDefinition) ExtractPrimaryKey(doc map[string]types.AttributeValue) (PrimaryKey, error) {
	part, ok := doc[def.PartitionKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("partition key not found")
	}
	if err := attributeMatchesDefinition(def.PartitionKey.Kind, part); err != nil {
		return PrimaryKey{}, fmt.Errorf("partition key kind does not match definition")
	}
	pk := PrimaryKey{
		Definition: def,
		Values: PrimaryKeyValues{
			PartitionKey: keyValueFromAV(part),
		},
	}
	if def.SortKey.Name == "" {
		return pk, nil
	}
	sort, ok := doc[def.SortKey.Name]
	if !ok {
		return PrimaryKey{}, fmt.Errorf("sort key not found")
	}
	if err := attributeMatchesDefinition(def.SortKey.Kind, sort); err != nil {
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
