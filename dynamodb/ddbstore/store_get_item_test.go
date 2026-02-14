package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_GetItem(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	t.Run("not found", func(t *testing.T) {
		_, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "nonexistent"},
				"sk": &types.AttributeValueMemberS{Value: "nonexistent"},
			},
		})
		require.Error(t, err)
	})

	t.Run("found after put", func(t *testing.T) {
		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "user#123"},
			"sk":   &types.AttributeValueMemberS{Value: "profile"},
			"name": &types.AttributeValueMemberS{Value: "John Doe"},
			"age":  &types.AttributeValueMemberN{Value: "30"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#123"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})
}

func TestStore_GetItem_ProjectionExpression(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	item := map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "user#1"},
		"sk":    &types.AttributeValueMemberS{Value: "profile"},
		"name":  &types.AttributeValueMemberS{Value: "John Doe"},
		"email": &types.AttributeValueMemberS{Value: "john@example.com"},
		"age":   &types.AttributeValueMemberN{Value: "30"},
		"address": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"city":    &types.AttributeValueMemberS{Value: "New York"},
			"country": &types.AttributeValueMemberS{Value: "USA"},
		}},
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "premium"},
			&types.AttributeValueMemberS{Value: "active"},
		}},
	}

	_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &singleTableDesign.Name,
		Item:      item,
	})
	require.NoError(t, err)

	t.Run("project single attribute", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#n"),
			ExpressionAttributeNames: map[string]string{
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: "John Doe"},
		}, result.Item)
	})

	t.Run("project multiple attributes", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#n, email, age"),
			ExpressionAttributeNames: map[string]string{
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]types.AttributeValue{
			"name":  &types.AttributeValueMemberS{Value: "John Doe"},
			"email": &types.AttributeValueMemberS{Value: "john@example.com"},
			"age":   &types.AttributeValueMemberN{Value: "30"},
		}, result.Item)
	})

	t.Run("project nested attribute", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("address.city"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]types.AttributeValue{
			"address": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"city": &types.AttributeValueMemberS{Value: "New York"},
			}},
		}, result.Item)
	})

	t.Run("project list element", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("tags[0]"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		// DynamoDB wraps list indices in a list
		assert.Equal(t, map[string]types.AttributeValue{
			"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "premium"},
			}},
		}, result.Item)
	})

	t.Run("project with expression attribute names", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#n, #e"),
			ExpressionAttributeNames: map[string]string{
				"#n": "name",
				"#e": "email",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]types.AttributeValue{
			"name":  &types.AttributeValueMemberS{Value: "John Doe"},
			"email": &types.AttributeValueMemberS{Value: "john@example.com"},
		}, result.Item)
	})

	t.Run("project nonexistent attribute silently ignored", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#n, nonexistent"),
			ExpressionAttributeNames: map[string]string{
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: "John Doe"},
		}, result.Item)
	})
}
