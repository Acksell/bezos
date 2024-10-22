package mockddb

import (
	"context"
	"testing"

	"bezos/bzoddb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

var singleTableDesign = table.TableDefinition{
	Name: "single-table-design-test",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	TimeToLiveKey: "ttl",
	GSIs: []table.TableDefinition{
		{
			IsGSI: true,
			Name:  "test-gsi",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "testgsipk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "testgsisk", Kind: table.KeyKindS},
			},
		},
	},
}

var testTableNoSortKey = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
	},
}

func TestPutItem(t *testing.T) {
	t.Run("success + failed condition", func(t *testing.T) {
		store := NewStore(singleTableDesign)
		ctx := context.Background()
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			TableName:           &singleTableDesign.Name,
			ConditionExpression: ptr("attribute_not_exists(pk)"),
		})
		require.NoError(t, err, "expected no error on first put")

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			TableName:           &singleTableDesign.Name,
			ConditionExpression: ptr("attribute_not_exists(pk)"),
		})
		require.Error(t, err, "condition should fail: document already exists")
	})
	t.Run("omitted sort key", func(t *testing.T) {
		store := NewStore(singleTableDesign)
		ctx := context.Background()
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "test"}},
			TableName: &singleTableDesign.Name,
		})
		require.ErrorContains(t, err, "sort key not found", "should error if sort key is required")

		store = NewStore(testTableNoSortKey)
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "test"}},
			TableName: &testTableNoSortKey.Name,
		})
		require.NoError(t, err, "shouldn't error if sort key is not required")
	})
}

func ptr(s string) *string {
	return &s
}
