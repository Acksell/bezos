package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_BatchWriteItem(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Batch put
	_, err := store.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			singleTableDesign.Name: {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "pk#1"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "pk#2"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				}}},
			},
		},
	})
	require.NoError(t, err)

	// Verify items exist
	result, err := store.Scan(ctx, &dynamodb.ScanInput{
		TableName: &singleTableDesign.Name,
	})
	require.NoError(t, err)
	assert.Len(t, result.Items, 2)

	// Batch delete
	_, err = store.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			singleTableDesign.Name: {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "pk#1"},
					"sk": &types.AttributeValueMemberS{Value: "sk"},
				}}},
			},
		},
	})
	require.NoError(t, err)

	result, err = store.Scan(ctx, &dynamodb.ScanInput{
		TableName: &singleTableDesign.Name,
	})
	require.NoError(t, err)
	assert.Len(t, result.Items, 1)
}
