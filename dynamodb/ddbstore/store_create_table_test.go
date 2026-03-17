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

func TestStore_CreateTable(t *testing.T) {
	t.Run("partition key only", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		out, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("simple-table"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, out.TableDescription)
		assert.Equal(t, "simple-table", *out.TableDescription.TableName)
		assert.Equal(t, types.TableStatusActive, out.TableDescription.TableStatus)

		// Verify table is usable with PutItem + GetItem.
		item := map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: "item1"},
			"data": &types.AttributeValueMemberS{Value: "hello"},
		}
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("simple-table"),
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("simple-table"),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "item1"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})

	t.Run("partition and sort key", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		out, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("composite-table"),
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
		assert.Equal(t, "composite-table", *out.TableDescription.TableName)

		// Verify the table works.
		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "user#1"},
			"sk":   &types.AttributeValueMemberN{Value: "100"},
			"name": &types.AttributeValueMemberS{Value: "Alice"},
		}
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("composite-table"),
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String("composite-table"),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#1"},
				"sk": &types.AttributeValueMemberN{Value: "100"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})

	t.Run("with GSI", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("gsi-table"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("gsi1pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("gsi1sk"), AttributeType: types.ScalarAttributeTypeS},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String("gsi1"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsi1pk"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("gsi1sk"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		})
		require.NoError(t, err)

		// Put an item and query it via the GSI.
		item := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#1"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#alice@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("gsi-table"),
			Item:      item,
		})
		require.NoError(t, err)

		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String("gsi-table"),
			IndexName:              aws.String("gsi1"),
			KeyConditionExpression: aws.String("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#alice@example.com"},
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		assert.Equal(t, item, result.Items[0])
	})

	t.Run("table already exists", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(singleTableDesign.Name),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		require.Error(t, err)
		var riue *types.ResourceInUseException
		assert.True(t, errors.As(err, &riue))
	})

	t.Run("missing table name", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		require.Error(t, err)
	})

	t.Run("missing key schema", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("bad-table"),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		require.Error(t, err)
	})

	t.Run("attribute not in definitions", func(t *testing.T) {
		store := newTestStore(t)
		ctx := context.Background()

		_, err := store.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String("bad-table"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("other"), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in AttributeDefinitions")
	})
}
