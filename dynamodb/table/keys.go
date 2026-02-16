package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PrimaryKeyDefinition struct {
	PartitionKey KeyDef
	SortKey      KeyDef // pointer to indicate optionality? or just focus on single table design where it's not optional?
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

// Type safety is ensured by using type constrained constructors generated based on the Table's KeyDefinition.
type PrimaryKeyValues struct {
	PartitionKey any
	SortKey      any
}

type PrimaryKey struct {
	Definition PrimaryKeyDefinition
	Values     PrimaryKeyValues
}

func (k PrimaryKey) DDB() map[string]types.AttributeValue {
	pk, err := attributevalue.Marshal(k.Values.PartitionKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal partition key of type %T with value %v: %w", k.Values.PartitionKey, k.Values.PartitionKey, err))
	}
	err = attributeMatchesDefinition(k.Definition.PartitionKey.Kind, pk)
	if err != nil {
		panic(fmt.Errorf("partition key kind does not match dynamo value: %w", err))
	}
	if k.Definition.SortKey.Name == "" {
		return map[string]types.AttributeValue{
			k.Definition.PartitionKey.Name: pk,
		}
	}
	if k.Values.SortKey == nil {
		// todo return errors instead
		panic(fmt.Errorf("sort key %q is required but got nil", k.Definition.SortKey.Name))
	}
	sk, err := attributevalue.Marshal(k.Values.SortKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal sort key of type %T with value %v: %w", k.Values.SortKey, k.Values.SortKey, err))
	}
	err = attributeMatchesDefinition(k.Definition.SortKey.Kind, sk)
	if err != nil {
		panic(fmt.Errorf("sort key %q kind does not match dynamo value: %w", k.Definition.SortKey.Name, err))
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
