package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_UpdateItem(t *testing.T) {
	t.Run("SET creates new item (upsert)", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
			UpdateExpression: ptrStr("SET #name = :name"),
			ExpressionAttributeNames: map[string]string{
				"#name": "name",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: "John"},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "John", got.Item["name"].(*types.AttributeValueMemberS).Value)
		// Verify key attributes are present
		assert.Equal(t, "user#1", got.Item["pk"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "profile", got.Item["sk"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("SET updates existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create initial item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "user#1"},
				"sk":   &types.AttributeValueMemberS{Value: "profile"},
				"name": &types.AttributeValueMemberS{Value: "John"},
				"age":  &types.AttributeValueMemberN{Value: "25"},
			},
		})
		require.NoError(t, err)

		// Update only the name
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
			UpdateExpression: ptrStr("SET #name = :name"),
			ExpressionAttributeNames: map[string]string{
				"#name": "name",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: "Jane"},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "Jane", got.Item["name"].(*types.AttributeValueMemberS).Value)
		// Verify other attributes preserved
		assert.Equal(t, "25", got.Item["age"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("SET with arithmetic", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item with counter
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "counter#1"},
				"sk":      &types.AttributeValueMemberS{Value: "data"},
				"counter": &types.AttributeValueMemberN{Value: "10"},
			},
		})
		require.NoError(t, err)

		// Increment counter
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "counter#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("SET #counter = #counter + :inc"),
			ExpressionAttributeNames: map[string]string{
				"#counter": "counter",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "5"},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "counter#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "15", got.Item["counter"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("SET with if_not_exists", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item without counter
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)

		// Use if_not_exists to set default counter
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("SET #counter = if_not_exists(#counter, :default)"),
			ExpressionAttributeNames: map[string]string{
				"#counter": "counter",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":default": &types.AttributeValueMemberN{Value: "0"},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0", got.Item["counter"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("REMOVE attribute", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":       &types.AttributeValueMemberS{Value: "user#1"},
				"sk":       &types.AttributeValueMemberS{Value: "profile"},
				"name":     &types.AttributeValueMemberS{Value: "John"},
				"toRemove": &types.AttributeValueMemberS{Value: "delete me"},
			},
		})
		require.NoError(t, err)

		// Remove attribute
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
			UpdateExpression: ptrStr("REMOVE toRemove"),
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "John", got.Item["name"].(*types.AttributeValueMemberS).Value)
		_, exists := got.Item["toRemove"]
		assert.False(t, exists, "toRemove should have been removed")
	})

	t.Run("ADD to number", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "item#1"},
				"sk":    &types.AttributeValueMemberS{Value: "data"},
				"count": &types.AttributeValueMemberN{Value: "10"},
			},
		})
		require.NoError(t, err)

		// ADD to number
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("ADD #count :inc"),
			ExpressionAttributeNames: map[string]string{
				"#count": "count",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "3"},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "13", got.Item["count"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("ADD to string set", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item with set
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "item#1"},
				"sk":   &types.AttributeValueMemberS{Value: "data"},
				"tags": &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			},
		})
		require.NoError(t, err)

		// ADD to set
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("ADD tags :newTags"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":newTags": &types.AttributeValueMemberSS{Value: []string{"c", "d"}},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, got.Item["tags"].(*types.AttributeValueMemberSS).Value)
	})

	t.Run("DELETE from string set", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item with set
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "item#1"},
				"sk":   &types.AttributeValueMemberS{Value: "data"},
				"tags": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c", "d"}},
			},
		})
		require.NoError(t, err)

		// DELETE from set
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("DELETE tags :removeTags"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":removeTags": &types.AttributeValueMemberSS{Value: []string{"b", "d"}},
			},
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"a", "c"}, got.Item["tags"].(*types.AttributeValueMemberSS).Value)
	})

	t.Run("with condition expression succeeds", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "item#1"},
				"sk":      &types.AttributeValueMemberS{Value: "data"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Update with condition
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression:    ptrStr("SET version = :newVer"),
			ConditionExpression: ptrStr("version = :oldVer"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":oldVer": &types.AttributeValueMemberN{Value: "1"},
				":newVer": &types.AttributeValueMemberN{Value: "2"},
			},
		})
		require.NoError(t, err)
	})

	t.Run("with condition expression fails", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "item#1"},
				"sk":      &types.AttributeValueMemberS{Value: "data"},
				"version": &types.AttributeValueMemberN{Value: "2"},
			},
		})
		require.NoError(t, err)

		// Update with failing condition
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression:    ptrStr("SET version = :newVer"),
			ConditionExpression: ptrStr("version = :oldVer"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":oldVer": &types.AttributeValueMemberN{Value: "1"}, // Wrong version
				":newVer": &types.AttributeValueMemberN{Value: "3"},
			},
		})
		require.Error(t, err)
	})

	t.Run("return values ALL_NEW", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "item#1"},
				"sk":   &types.AttributeValueMemberS{Value: "data"},
				"name": &types.AttributeValueMemberS{Value: "old"},
			},
		})
		require.NoError(t, err)

		result, err := store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("SET #name = :name"),
			ExpressionAttributeNames: map[string]string{
				"#name": "name",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: "new"},
			},
			ReturnValues: types.ReturnValueAllNew,
		})
		require.NoError(t, err)
		assert.Equal(t, "new", result.Attributes["name"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("return values ALL_OLD", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "item#1"},
				"sk":   &types.AttributeValueMemberS{Value: "data"},
				"name": &types.AttributeValueMemberS{Value: "old"},
			},
		})
		require.NoError(t, err)

		result, err := store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item#1"},
				"sk": &types.AttributeValueMemberS{Value: "data"},
			},
			UpdateExpression: ptrStr("SET #name = :name"),
			ExpressionAttributeNames: map[string]string{
				"#name": "name",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":name": &types.AttributeValueMemberS{Value: "new"},
			},
			ReturnValues: types.ReturnValueAllOld,
		})
		require.NoError(t, err)
		assert.Equal(t, "old", result.Attributes["name"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("updates GSI", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item with GSI keys
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: "user#1"},
				"sk":     &types.AttributeValueMemberS{Value: "profile"},
				"gsi1pk": &types.AttributeValueMemberS{Value: "email#old@test.com"},
				"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
			},
		})
		require.NoError(t, err)

		// Update GSI key
		_, err = store.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
			UpdateExpression: ptrStr("SET gsi1pk = :newEmail"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":newEmail": &types.AttributeValueMemberS{Value: "email#new@test.com"},
			},
		})
		require.NoError(t, err)

		gsiName := "gsi1"

		// Old GSI key should not find anything
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#old@test.com"},
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
				":pk": &types.AttributeValueMemberS{Value: "email#new@test.com"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 1)
	})
}
