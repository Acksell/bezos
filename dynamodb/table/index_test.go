package table

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

var IndexByID = PrimaryIndexDefinition{
	Table:          pkOnlyTable,
	PartitionKeyer: FmtKeyer("ID#%s", "id"),
}

var IndexByIDAndVersion = PrimaryIndexDefinition{
	Table:          pkAndSKTable,
	PartitionKeyer: FmtKeyer("ID#%s", "id"),
	SortKeyer:      FmtKeyer("VERSION#%s", "meta.version"),
}

var pkOnlyTable = TableDefinition{
	KeyDefinitions: PrimaryKeyDefinition{
		PartitionKey: KeyDef{
			Name: "pk",
			Kind: KeyKindS,
		},
	},
}

var pkAndSKTable = TableDefinition{
	KeyDefinitions: PrimaryKeyDefinition{
		PartitionKey: KeyDef{
			Name: "pk",
			Kind: KeyKindS,
		},
		SortKey: KeyDef{
			Name: "sk",
			Kind: KeyKindS,
		},
	},
}

func TestIndexPrimaryKey(t *testing.T) {
	t.Run("fmt keyer resolves value", func(t *testing.T) {
		doc := map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "123"},
		}
		pk, err := IndexByID.PrimaryKey(doc)
		require.NoError(t, err, "unexpected error")
		require.Equal(t, "pk", pk.Definition.PartitionKey.Name, "unexpected partition key name")
		require.Equal(t, "ID#123", pk.Values.PartitionKey, "unexpected partition key")
		require.Nil(t, pk.Values.SortKey, "unexpected sort key")
	})
	t.Run("fmt keyer ignores other values", func(t *testing.T) {
		doc := map[string]types.AttributeValue{
			"id":         &types.AttributeValueMemberS{Value: "123"},
			"anotherone": &types.AttributeValueMemberS{Value: "foo"},
			"anothertwo": &types.AttributeValueMemberS{Value: "bar"},
		}
		pk, err := IndexByID.PrimaryKey(doc)
		require.NoError(t, err, "unexpected error")
		require.Equal(t, "ID#123", pk.Values.PartitionKey, "unexpected partition key")
		require.Nil(t, pk.Values.SortKey, "unexpected sort key")
	})
	t.Run("fmt keyer nested values", func(t *testing.T) {
		doc := map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "123"},
			"meta": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"version": &types.AttributeValueMemberS{Value: "456"},
			}},
			"foo": &types.AttributeValueMemberS{Value: "bar"},
		}
		pk, err := IndexByIDAndVersion.PrimaryKey(doc)
		require.NoError(t, err, "unexpected error")
		require.Equal(t, "ID#123", pk.Values.PartitionKey, "unexpected partition key")
		require.Equal(t, "VERSION#456", pk.Values.SortKey, "unexpected sort key")
	})
}
