package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_DeleteItem(t *testing.T) {
	t.Run("delete existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test"},
			"sk": &types.AttributeValueMemberS{Value: "test"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		_, err = store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, result.Item) // Should not be found
	})

	t.Run("return old values", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		result, err := store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ReturnValues: types.ReturnValueAllOld,
		})
		require.NoError(t, err)
		assert.Equal(t, item, result.Attributes)
	})

	t.Run("delete nonexistent item succeeds", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "nonexistent"},
				"sk": &types.AttributeValueMemberS{Value: "nonexistent"},
			},
		})
		require.NoError(t, err)
	})
}
