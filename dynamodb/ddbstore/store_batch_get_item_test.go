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

func TestStore_BatchGetItem(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items
	for i := 0; i < 5; i++ {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
				"sk":   &types.AttributeValueMemberS{Value: "sk"},
				"data": &types.AttributeValueMemberS{Value: fmt.Sprintf("data-%d", i)},
			},
		})
		require.NoError(t, err)
	}

	result, err := store.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			singleTableDesign.Name: {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "pk#0"}, "sk": &types.AttributeValueMemberS{Value: "sk"}},
					{"pk": &types.AttributeValueMemberS{Value: "pk#2"}, "sk": &types.AttributeValueMemberS{Value: "sk"}},
					{"pk": &types.AttributeValueMemberS{Value: "pk#4"}, "sk": &types.AttributeValueMemberS{Value: "sk"}},
					{"pk": &types.AttributeValueMemberS{Value: "nonexistent"}, "sk": &types.AttributeValueMemberS{Value: "sk"}},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, result.Responses[singleTableDesign.Name], 3)
}

func TestStore_BatchGetItem_ProjectionExpression(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items
	items := []map[string]types.AttributeValue{
		{
			"pk":   &types.AttributeValueMemberS{Value: "item#1"},
			"sk":   &types.AttributeValueMemberS{Value: "data"},
			"name": &types.AttributeValueMemberS{Value: "First"},
			"desc": &types.AttributeValueMemberS{Value: "Description 1"},
		},
		{
			"pk":   &types.AttributeValueMemberS{Value: "item#2"},
			"sk":   &types.AttributeValueMemberS{Value: "data"},
			"name": &types.AttributeValueMemberS{Value: "Second"},
			"desc": &types.AttributeValueMemberS{Value: "Description 2"},
		},
	}

	for _, item := range items {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)
	}

	t.Run("project attributes from batch get", func(t *testing.T) {
		result, err := store.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				singleTableDesign.Name: {
					Keys: []map[string]types.AttributeValue{
						{
							"pk": &types.AttributeValueMemberS{Value: "item#1"},
							"sk": &types.AttributeValueMemberS{Value: "data"},
						},
						{
							"pk": &types.AttributeValueMemberS{Value: "item#2"},
							"sk": &types.AttributeValueMemberS{Value: "data"},
						},
					},
					ProjectionExpression: ptrStr("#n"),
					ExpressionAttributeNames: map[string]string{
						"#n": "name",
					},
				},
			},
		})
		require.NoError(t, err)

		tableResults := result.Responses[singleTableDesign.Name]
		assert.Len(t, tableResults, 2)

		for _, item := range tableResults {
			assert.Len(t, item, 1)
			assert.Contains(t, item, "name")
			assert.NotContains(t, item, "pk")
			assert.NotContains(t, item, "sk")
			assert.NotContains(t, item, "desc")
		}
	})
}
