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

func TestStore_TransactGetItems(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items
	for i := 0; i < 3; i++ {
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

	result, err := store.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{
				TableName: &singleTableDesign.Name,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "pk#0"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				},
			}},
			{Get: &types.Get{
				TableName: &singleTableDesign.Name,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "pk#1"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				},
			}},
			{Get: &types.Get{
				TableName: &singleTableDesign.Name,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "nonexistent"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Responses, 3)
	assert.NotNil(t, result.Responses[0].Item)
	assert.NotNil(t, result.Responses[1].Item)
	assert.Nil(t, result.Responses[2].Item) // nonexistent
}

func TestStore_TransactGetItems_ProjectionExpression(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items
	items := []map[string]types.AttributeValue{
		{
			"pk":    &types.AttributeValueMemberS{Value: "tx#1"},
			"sk":    &types.AttributeValueMemberS{Value: "data"},
			"field": &types.AttributeValueMemberS{Value: "value1"},
			"extra": &types.AttributeValueMemberS{Value: "extra1"},
		},
		{
			"pk":    &types.AttributeValueMemberS{Value: "tx#2"},
			"sk":    &types.AttributeValueMemberS{Value: "data"},
			"field": &types.AttributeValueMemberS{Value: "value2"},
			"extra": &types.AttributeValueMemberS{Value: "extra2"},
		},
	}

	for _, item := range items {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)
	}

	t.Run("project attributes from transact get", func(t *testing.T) {
		result, err := store.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName:            &singleTableDesign.Name,
						ProjectionExpression: ptrStr("field"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx#1"},
							"sk": &types.AttributeValueMemberS{Value: "data"},
						},
					},
				},
				{
					Get: &types.Get{
						TableName:            &singleTableDesign.Name,
						ProjectionExpression: ptrStr("pk, field"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx#2"},
							"sk": &types.AttributeValueMemberS{Value: "data"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		assert.Len(t, result.Responses, 2)

		// First item should only have 'field'
		assert.Equal(t, map[string]types.AttributeValue{
			"field": &types.AttributeValueMemberS{Value: "value1"},
		}, result.Responses[0].Item)

		// Second item should have 'pk' and 'field'
		assert.Equal(t, map[string]types.AttributeValue{
			"pk":    &types.AttributeValueMemberS{Value: "tx#2"},
			"field": &types.AttributeValueMemberS{Value: "value2"},
		}, result.Responses[1].Item)
	})
}
