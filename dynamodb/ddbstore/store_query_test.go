package ddbstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Query(t *testing.T) {
	t.Run("query by partition key only", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Insert multiple items with same partition key
		items := []map[string]types.AttributeValue{
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "a"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "b"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "c"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#2"}, "sk": &types.AttributeValueMemberS{Value: "a"}},
		}

		for _, item := range items {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item:      item,
			})
			require.NoError(t, err)
		}

		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "user#1"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 3)
	})

	t.Run("query with sort key condition", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		items := []map[string]types.AttributeValue{
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "order#001"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "order#002"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "profile"}},
		}

		for _, item := range items {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item:      item,
			})
			require.NoError(t, err)
		}

		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk AND begins_with(sk, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "user#1"},
				":prefix": &types.AttributeValueMemberS{Value: "order#"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 2)
	})

	t.Run("query with numeric sort key", func(t *testing.T) {
		store := newTestStore(t, numericSortKeyTable)
		ctx := context.Background()

		// Insert items with numeric sort keys
		for i := 1; i <= 10; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &numericSortKeyTable.Name,
				Item: map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: "partition"},
					"sk":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
					"data": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				},
			})
			require.NoError(t, err)
		}

		// Query with BETWEEN
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &numericSortKeyTable.Name,
			KeyConditionExpression: ptrStr("pk = :pk AND sk BETWEEN :lo AND :hi"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "partition"},
				":lo": &types.AttributeValueMemberN{Value: "3"},
				":hi": &types.AttributeValueMemberN{Value: "7"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 5)
	})

	t.Run("query with limit", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		for i := 0; i < 10; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "user#1"},
					"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%02d", i)},
				},
			})
			require.NoError(t, err)
		}

		limit := int32(3)
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "user#1"},
			},
			Limit: &limit,
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 3)
		assert.NotNil(t, result.LastEvaluatedKey)
	})

	t.Run("query in reverse order", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "user#1"},
					"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%02d", i)},
				},
			})
			require.NoError(t, err)
		}

		scanForward := false
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "user#1"},
			},
			ScanIndexForward: &scanForward,
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 5)

		// Verify order is reversed
		assert.Equal(t, "item#04", result.Items[0]["sk"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "item#00", result.Items[4]["sk"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("query with filter expression", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		items := []map[string]types.AttributeValue{
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "a"}, "status": &types.AttributeValueMemberS{Value: "active"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "b"}, "status": &types.AttributeValueMemberS{Value: "inactive"}},
			{"pk": &types.AttributeValueMemberS{Value: "user#1"}, "sk": &types.AttributeValueMemberS{Value: "c"}, "status": &types.AttributeValueMemberS{Value: "active"}},
		}

		for _, item := range items {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item:      item,
			})
			require.NoError(t, err)
		}

		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			FilterExpression:       ptrStr("#status = :status"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "user#1"},
				":status": &types.AttributeValueMemberS{Value: "active"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 2)
	})
}

func TestStore_Query_ProjectionExpression(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert multiple items
	items := []map[string]types.AttributeValue{
		{
			"pk":    &types.AttributeValueMemberS{Value: "user#1"},
			"sk":    &types.AttributeValueMemberS{Value: "order#001"},
			"total": &types.AttributeValueMemberN{Value: "100"},
			"item":  &types.AttributeValueMemberS{Value: "Widget"},
		},
		{
			"pk":    &types.AttributeValueMemberS{Value: "user#1"},
			"sk":    &types.AttributeValueMemberS{Value: "order#002"},
			"total": &types.AttributeValueMemberN{Value: "200"},
			"item":  &types.AttributeValueMemberS{Value: "Gadget"},
		},
	}

	for _, item := range items {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)
	}

	t.Run("project attributes from query results", func(t *testing.T) {
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			KeyConditionExpression: ptrStr("pk = :pk"),
			ProjectionExpression:   ptrStr("sk, #t"),
			ExpressionAttributeNames: map[string]string{
				"#t": "total",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "user#1"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 2)

		// Verify only projected attributes are returned
		for _, item := range result.Items {
			assert.Len(t, item, 2)
			assert.Contains(t, item, "sk")
			assert.Contains(t, item, "total")
			assert.NotContains(t, item, "pk")
			assert.NotContains(t, item, "item")
		}
	})
}
