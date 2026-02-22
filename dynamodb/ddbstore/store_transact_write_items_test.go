package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_TransactWriteItems(t *testing.T) {
	t.Run("atomic write succeeds", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx#1"},
							"sk": &types.AttributeValueMemberS{Value: "a"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx#1"},
							"sk": &types.AttributeValueMemberS{Value: "b"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "tx#1"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 2)
	})

	t.Run("transaction rollback on condition failure", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create existing item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "existing"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)

		// Transaction with condition that will fail
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "new#1"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName:           &singleTableDesign.Name,
						ConditionExpression: ptrStr("attribute_not_exists(pk)"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "existing"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
					},
				},
			},
		})
		require.Error(t, err)

		// Verify first item was NOT created due to rollback
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "new#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, result.Item) // Should not exist
	})

	t.Run("condition check without write", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item to check
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "check"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Transaction with condition check
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					ConditionCheck: &types.ConditionCheck{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "check"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						ConditionExpression: ptrStr("version = :v"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":v": &types.AttributeValueMemberN{Value: "1"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "new"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify new item was created
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "new"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, got.Item)
	})

	t.Run("update in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create items to update
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-update#1"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "10"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-update#2"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "20"},
			},
		})
		require.NoError(t, err)

		// Transaction with updates
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-update#1"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count + :inc"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":inc": &types.AttributeValueMemberN{Value: "5"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-update#2"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count - :dec"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":dec": &types.AttributeValueMemberN{Value: "3"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify both updates applied
		got1, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-update#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "15", got1.Item["count"].(*types.AttributeValueMemberN).Value)

		got2, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-update#2"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "17", got2.Item["count"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("update with condition in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-cond-update"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Transaction with conditional update - should succeed
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-cond-update"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression:    ptrStr("SET version = :newVer"),
						ConditionExpression: ptrStr("version = :oldVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":oldVer": &types.AttributeValueMemberN{Value: "1"},
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify update applied
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-cond-update"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "2", got.Item["version"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("update condition failure rolls back transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create items
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-rollback#1"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-rollback#2"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "5"}, // Wrong version for condition
			},
		})
		require.NoError(t, err)

		// Transaction with update that will fail condition
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-rollback#1"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET version = :newVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-rollback#2"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression:    ptrStr("SET version = :newVer"),
						ConditionExpression: ptrStr("version = :oldVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":oldVer": &types.AttributeValueMemberN{Value: "1"}, // Will fail - actual is 5
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
			},
		})
		require.Error(t, err)

		// Verify first update was NOT applied due to rollback
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-rollback#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "1", got.Item["version"].(*types.AttributeValueMemberN).Value) // Still 1, not 2
	})

	t.Run("update creates new item in transaction (upsert)", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Transaction with update on non-existent item
		_, err := store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-upsert"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #name = :name"),
						ExpressionAttributeNames: map[string]string{
							"#name": "name",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":name": &types.AttributeValueMemberS{Value: "created"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify item was created
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-upsert"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "created", got.Item["name"].(*types.AttributeValueMemberS).Value)
		// Verify key attributes present
		assert.Equal(t, "tx-upsert", got.Item["pk"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "item", got.Item["sk"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("mixed put update delete in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item to update and delete
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-mixed-update"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "100"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)

		// Mixed transaction
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "tx-mixed-put"},
							"sk":   &types.AttributeValueMemberS{Value: "item"},
							"data": &types.AttributeValueMemberS{Value: "new"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-mixed-update"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count + :inc"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":inc": &types.AttributeValueMemberN{Value: "50"},
						},
					},
				},
				{
					Delete: &types.Delete{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify put
		gotPut, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-put"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "new", gotPut.Item["data"].(*types.AttributeValueMemberS).Value)

		// Verify update
		gotUpdate, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-update"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "150", gotUpdate.Item["count"].(*types.AttributeValueMemberN).Value)

		// Verify delete
		deleteResult, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Empty(t, deleteResult.Item) // Should not exist
	})
}
