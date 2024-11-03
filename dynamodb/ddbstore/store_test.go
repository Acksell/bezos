package ddbstore

import (
	"context"
	"testing"

	"bezos/dynamodb/table"

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

	t.Run("gsi", func(t *testing.T) {
		store := NewStore(singleTableDesign)
		ctx := context.Background()
		tabl, err := store.getTable(&singleTableDesign.Name)
		require.NoError(t, err)
		gsi := tabl.gsis["test-gsi"]

		t.Run("is inserted to GSI", func(t *testing.T) {
			item := map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: "test"},
				"sk":        &types.AttributeValueMemberS{Value: "test"},
				"testgsipk": &types.AttributeValueMemberS{Value: "testgsi_pkval"},
				"testgsisk": &types.AttributeValueMemberS{Value: "testgsi_skval"},
			}
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				Item:      item,
				TableName: &singleTableDesign.Name,
			})
			require.NoError(t, err, "should be able to insert item")

			got, err := gsi.GetItem(ctx, &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"testgsipk": &types.AttributeValueMemberS{Value: "testgsi_pkval"},
					"testgsisk": &types.AttributeValueMemberS{Value: "testgsi_skval"},
				},
				TableName: &singleTableDesign.Name,
			})
			require.NoError(t, err, "should be able to fetch item from GSI")
			require.Equal(t, item, got.Item, "item should be inserted and fetchable via GetItem on GSI")
		})
		t.Run("deleted old and adds new item to GSI", func(t *testing.T) {
			item := map[string]types.AttributeValue{
				"pk":        &types.AttributeValueMemberS{Value: "test"},
				"sk":        &types.AttributeValueMemberS{Value: "test"},
				"testgsipk": &types.AttributeValueMemberS{Value: "testgsi_pkval"},
				"testgsisk": &types.AttributeValueMemberS{Value: "testgsi_skval_NEW"},
			}
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				Item:      item,
				TableName: &singleTableDesign.Name,
			})
			require.NoError(t, err, "should be able to insert item")

			_, err = gsi.GetItem(ctx, &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"testgsipk": &types.AttributeValueMemberS{Value: "testgsi_pkval"},
					"testgsisk": &types.AttributeValueMemberS{Value: "testgsi_skval_NEW"},
				},
				TableName: &singleTableDesign.Name,
			})
			require.NoError(t, err, "expected to be found")

			_, err = gsi.GetItem(ctx, &dynamodb.GetItemInput{
				Key: map[string]types.AttributeValue{
					"testgsipk": &types.AttributeValueMemberS{Value: "testgsi_pkval"},
					"testgsisk": &types.AttributeValueMemberS{Value: "testgsi_skval"},
				},
				TableName: &singleTableDesign.Name,
			})
			require.Error(t, err, "expected to be deleted")

		})
	})
}

func ptr(s string) *string {
	return &s
}
