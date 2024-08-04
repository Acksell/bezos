package normddb

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

// TODO test
func (k PrimaryKey) DDB() map[string]types.AttributeValue {
	pk, err := attributevalue.Marshal(k.Values.PartitionKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal partition key of type %T with value %v: %w", k.Values.PartitionKey, k.Values.PartitionKey, err))
	}
	err = attributeMatchesDefinition(k.Definition.PartitionKey.Kind, pk)
	if err != nil {
		panic(fmt.Errorf("key kind does not match dynamo value: %w", err))
	}

	sk, err := attributevalue.Marshal(k.Values.PartitionKey)
	if err != nil {
		panic(fmt.Errorf("failed to marshal sort key of type %T with value %v: %w", k.Values.PartitionKey, k.Values.PartitionKey, err))
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

//--------------------------------------------------------------------

// type KeyType interface {
// 	string | number | []byte
// }

// // A primary key is a combination of a partition key and a sort key.
// // A primary key can be used on both the primary table but also a secondary index.
// type PrimaryKey[PK KeyType, SK KeyType] struct {
// 	Spec   PrimaryKeySpec
// 	Values PrimaryKeyValues[PK, SK]
// }

// func toAttributeValue[K KeyType](key K) types.AttributeValue {
// 	var av types.AttributeValue
// 	switch pk := any(key).(type) {
// 	case string:
// 		av = &types.AttributeValueMemberS{Value: pk}
// 	case int, int8, int16, int32, int64, uint, uint16, uint32, uint64:
// 		v := fmt.Sprintf("%d", key)
// 		av = &types.AttributeValueMemberN{Value: v}
// 	case float32, float64:
// 		v := fmt.Sprintf("%f", key) // todo is there a float format mismatch bug here with json?
// 		av = &types.AttributeValueMemberN{Value: v}
// 	case []byte:
// 		av = &types.AttributeValueMemberB{Value: pk}
// 	default:
// 		panic(fmt.Sprintf("unsupported type %T", pk))
// 	}
// 	return av
// }

// func (k PrimaryKey[PK, SK]) DDB() map[string]types.AttributeValue {
// 	return map[string]types.AttributeValue{
// 		k.Spec.PartitionKey.Name: toAttributeValue(k.Values.PartitionKey),
// 		k.Spec.SortKey.Name:      toAttributeValue(k.Values.SortKey),
// 	}
// }

// type PrimaryKeySpec struct {
// 	PartitionKey KeySpec
// 	SortKey      KeySpec
// }

// type KeySpec struct {
// 	Name string
// 	// optional pattern if type is string
// 	Pattern string
// }

// type PrimaryKeyValues[PK KeyType, SK KeyType] struct {
// 	PartitionKey PK
// 	SortKey      SK
// }

// old implementation where only string values were supported
// package normddb

// import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// // A primary key is a combination of a partition key and a sort key.
// // A primary key can be used on both the primary table but also a secondary index.
// type PrimaryKey struct {
// 	Names  KeyNames
// 	Values KeyValues
// }

// func (pk PrimaryKey) DDB() map[string]types.AttributeValue {
// 	return map[string]types.AttributeValue{
// 		pk.Names.PartitionKeyName: &types.AttributeValueMemberS{Value: pk.Values.PartitionKey},
// 		pk.Names.SortKeyName:      &types.AttributeValueMemberS{Value: pk.Values.SortKey},
// 	}
// }

// type KeyNames struct {
// 	PartitionKeyName string
// 	SortKeyName      string
// }

// // Since string key values are so common, we use this as the default
// type KeyValues struct {
// 	PartitionKey string
// 	SortKey      string
// }
