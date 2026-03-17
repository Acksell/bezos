package ddbstore

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_DescribeTable(t *testing.T) {
	t.Run("static table", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		out, err := store.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(singleTableDesign.Name),
		})
		require.NoError(t, err)
		require.NotNil(t, out.Table)

		desc := out.Table
		assert.Equal(t, singleTableDesign.Name, *desc.TableName)
		assert.Equal(t, types.TableStatusActive, desc.TableStatus)

		// Check key schema.
		require.Len(t, desc.KeySchema, 2)
		var hashKey, rangeKey *types.KeySchemaElement
		for i := range desc.KeySchema {
			switch desc.KeySchema[i].KeyType {
			case types.KeyTypeHash:
				hashKey = &desc.KeySchema[i]
			case types.KeyTypeRange:
				rangeKey = &desc.KeySchema[i]
			}
		}
		require.NotNil(t, hashKey)
		assert.Equal(t, "pk", *hashKey.AttributeName)
		require.NotNil(t, rangeKey)
		assert.Equal(t, "sk", *rangeKey.AttributeName)

		// Check GSI.
		require.Len(t, desc.GlobalSecondaryIndexes, 1)
		assert.Equal(t, "gsi1", *desc.GlobalSecondaryIndexes[0].IndexName)
		assert.Equal(t, types.IndexStatusActive, desc.GlobalSecondaryIndexes[0].IndexStatus)
	})

	t.Run("dynamically created table", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("dynamic-table"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			},
		})
		require.NoError(t, err)

		out, err := store.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String("dynamic-table"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.Table)

		assert.Equal(t, "dynamic-table", *out.Table.TableName)
		assert.Equal(t, types.TableStatusActive, out.Table.TableStatus)
		require.Len(t, out.Table.KeySchema, 2)

		// Verify attribute definitions include both keys.
		attrNames := make(map[string]types.ScalarAttributeType)
		for _, ad := range out.Table.AttributeDefinitions {
			attrNames[*ad.AttributeName] = ad.AttributeType
		}
		assert.Equal(t, types.ScalarAttributeTypeS, attrNames["pk"])
		assert.Equal(t, types.ScalarAttributeTypeN, attrNames["sk"])
	})

	t.Run("table without sort key", func(t *testing.T) {
		store := newTestStore(t, noSortKeyTable)
		ctx := context.Background()

		out, err := store.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(noSortKeyTable.Name),
		})
		require.NoError(t, err)
		require.Len(t, out.Table.KeySchema, 1)
		assert.Equal(t, types.KeyTypeHash, out.Table.KeySchema[0].KeyType)
		assert.Equal(t, "pk", *out.Table.KeySchema[0].AttributeName)
	})

	t.Run("table not found", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String("nonexistent"),
		})
		require.Error(t, err)
		var rnfe *types.ResourceNotFoundException
		assert.True(t, errors.As(err, &rnfe))
	})

	t.Run("missing table name", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.DescribeTable(ctx, &dynamodb.DescribeTableInput{})
		require.Error(t, err)
	})
}
