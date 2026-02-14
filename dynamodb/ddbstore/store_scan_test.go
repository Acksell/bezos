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

func TestStore_Scan(t *testing.T) {
	t.Run("scan all items", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				},
			})
			require.NoError(t, err)
		}

		result, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName: &singleTableDesign.Name,
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 5)
	})

	t.Run("scan with filter", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item: map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
					"sk":   &types.AttributeValueMemberS{Value: "sk"},
					"even": &types.AttributeValueMemberBOOL{Value: i%2 == 0},
				},
			})
			require.NoError(t, err)
		}

		result, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName:        &singleTableDesign.Name,
			FilterExpression: ptrStr("even = :val"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":val": &types.AttributeValueMemberBOOL{Value: true},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 3) // 0, 2, 4
	})

	t.Run("scan with limit and pagination", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		for i := 0; i < 10; i++ {
			_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: &singleTableDesign.Name,
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%02d", i)},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				},
			})
			require.NoError(t, err)
		}

		// First page
		limit := int32(3)
		result1, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName: &singleTableDesign.Name,
			Limit:     &limit,
		})
		require.NoError(t, err)
		assert.Len(t, result1.Items, 3)
		assert.NotNil(t, result1.LastEvaluatedKey)

		// Second page
		result2, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName:         &singleTableDesign.Name,
			Limit:             &limit,
			ExclusiveStartKey: result1.LastEvaluatedKey,
		})
		require.NoError(t, err)
		assert.Len(t, result2.Items, 3)

		// Verify no overlap
		for _, item1 := range result1.Items {
			for _, item2 := range result2.Items {
				assert.NotEqual(t, item1["pk"], item2["pk"])
			}
		}
	})
}

func TestStore_Scan_ProjectionExpression(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items
	items := []map[string]types.AttributeValue{
		{
			"pk":     &types.AttributeValueMemberS{Value: "a"},
			"sk":     &types.AttributeValueMemberS{Value: "1"},
			"field1": &types.AttributeValueMemberS{Value: "value1"},
			"field2": &types.AttributeValueMemberS{Value: "value2"},
		},
		{
			"pk":     &types.AttributeValueMemberS{Value: "b"},
			"sk":     &types.AttributeValueMemberS{Value: "2"},
			"field1": &types.AttributeValueMemberS{Value: "value3"},
			"field2": &types.AttributeValueMemberS{Value: "value4"},
		},
	}

	for _, item := range items {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)
	}

	t.Run("project attributes from scan results", func(t *testing.T) {
		result, err := store.Scan(ctx, &dynamodb.ScanInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("pk, field1"),
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 2)

		for _, item := range result.Items {
			assert.Len(t, item, 2)
			assert.Contains(t, item, "pk")
			assert.Contains(t, item, "field1")
			assert.NotContains(t, item, "sk")
			assert.NotContains(t, item, "field2")
		}
	})
}
