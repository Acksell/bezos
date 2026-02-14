package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_PutItem(t *testing.T) {
	t.Run("simple put and retrieve", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello world"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})

	t.Run("overwrite existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "original"},
		}
		item2 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "updated"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item2,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item2, got.Item)
	})

	t.Run("return old values", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "original"},
		}
		item2 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "updated"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		result, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:    &singleTableDesign.Name,
			Item:         item2,
			ReturnValues: types.ReturnValueAllOld,
		})
		require.NoError(t, err)
		assert.Equal(t, item1, result.Attributes)
	})

	t.Run("missing sort key errors", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sort key")
	})

	t.Run("table without sort key", func(t *testing.T) {
		store := newTestStore(t, noSortKeyTable)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &noSortKeyTable.Name,
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &noSortKeyTable.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})
}

func TestStore_ConditionExpressions(t *testing.T) {
	t.Run("attribute_not_exists succeeds on new item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ConditionExpression: ptrStr("attribute_not_exists(pk)"),
		})
		require.NoError(t, err)
	})

	t.Run("attribute_not_exists fails on existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ConditionExpression: ptrStr("attribute_not_exists(pk)"),
		})
		require.Error(t, err)
	})

	t.Run("attribute_exists succeeds on existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "test"},
				"sk":   &types.AttributeValueMemberS{Value: "test"},
				"data": &types.AttributeValueMemberS{Value: "updated"},
			},
			ConditionExpression: ptrStr("attribute_exists(pk)"),
		})
		require.NoError(t, err)
	})

	t.Run("comparison condition", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Should succeed: version = 1
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "2"},
			},
			ConditionExpression:       ptrStr("version = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "1"}},
		})
		require.NoError(t, err)

		// Should fail: version is now 2, not 1
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "3"},
			},
			ConditionExpression:       ptrStr("version = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "1"}},
		})
		require.Error(t, err)
	})
}

func TestStore_GSI(t *testing.T) {
	t.Run("item inserted to GSI", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#john@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
			"name":   &types.AttributeValueMemberS{Value: "John"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		// Query main table
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#123"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)

		// Query GSI
		gsiName := "gsi1"
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#john@example.com"},
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		assert.Equal(t, item, result.Items[0])
	})

	t.Run("GSI updated when keys change", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#old@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		item2 := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#new@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item2,
		})
		require.NoError(t, err)

		gsiName := "gsi1"

		// Old GSI key should not find anything
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#old@example.com"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 0)

		// New GSI key should find the item
		result, err = store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#new@example.com"},
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
	})

	t.Run("GSI entry deleted when main item deleted", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#test@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		_, err = store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#123"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)

		gsiName := "gsi1"
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#test@example.com"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 0)
	})
}
