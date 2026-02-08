package ddbstore

import (
	"context"
	"fmt"
	"testing"

	"bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test table definitions
var singleTableDesign = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	GSIs: []table.TableDefinition{
		{
			IsGSI: true,
			Name:  "gsi1",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

var numericSortKeyTable = table.TableDefinition{
	Name: "numeric-sk-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindN},
	},
}

var noSortKeyTable = table.TableDefinition{
	Name: "no-sk-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
	},
}

func newTestStore(t *testing.T, defs ...table.TableDefinition) *Store {
	store, err := New(StoreOptions{InMemory: true}, defs...)
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

// =============================================================================
// Basic CRUD Operations
// =============================================================================

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

func TestStore_PutItem(t *testing.T) {
	t.Run("simple put and retrieve", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello world"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})

	t.Run("overwrite existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "original"},
		}
		item2 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "updated"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item2,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item2, got.Item)
	})

	t.Run("return old values", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "original"},
		}
		item2 := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "updated"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		result, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:    &singleTableDesign.Name,
			Item:         item2,
			ReturnValues: types.ReturnValueAllOld,
		})
		require.NoError(t, err)
		assert.Equal(t, item1, result.Attributes)
	})

	t.Run("missing sort key errors", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sort key")
	})

	t.Run("table without sort key", func(t *testing.T) {
		store := newTestStore(t, noSortKeyTable)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &noSortKeyTable.Name,
			Item:      item,
		})
		require.NoError(t, err)

		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &noSortKeyTable.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)
	})
}

func TestStore_DeleteItem(t *testing.T) {
	t.Run("delete existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test"},
			"sk": &types.AttributeValueMemberS{Value: "test"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		_, err = store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		_, err = store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.Error(t, err) // Should not be found
	})

	t.Run("return old values", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "test"},
			"sk":   &types.AttributeValueMemberS{Value: "test"},
			"data": &types.AttributeValueMemberS{Value: "hello"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		result, err := store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ReturnValues: types.ReturnValueAllOld,
		})
		require.NoError(t, err)
		assert.Equal(t, item, result.Attributes)
	})

	t.Run("delete nonexistent item succeeds", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "nonexistent"},
				"sk": &types.AttributeValueMemberS{Value: "nonexistent"},
			},
		})
		require.NoError(t, err)
	})
}

// =============================================================================
// Condition Expressions
// =============================================================================

func TestStore_ConditionExpressions(t *testing.T) {
	t.Run("attribute_not_exists succeeds on new item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ConditionExpression: ptrStr("attribute_not_exists(pk)"),
		})
		require.NoError(t, err)
	})

	t.Run("attribute_not_exists fails on existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
			ConditionExpression: ptrStr("attribute_not_exists(pk)"),
		})
		require.Error(t, err)
	})

	t.Run("attribute_exists succeeds on existing item", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: "test"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "test"},
				"sk":   &types.AttributeValueMemberS{Value: "test"},
				"data": &types.AttributeValueMemberS{Value: "updated"},
			},
			ConditionExpression: ptrStr("attribute_exists(pk)"),
		})
		require.NoError(t, err)
	})

	t.Run("comparison condition", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Should succeed: version = 1
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "2"},
			},
			ConditionExpression:       ptrStr("version = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "1"}},
		})
		require.NoError(t, err)

		// Should fail: version is now 2, not 1
		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "test"},
				"sk":      &types.AttributeValueMemberS{Value: "test"},
				"version": &types.AttributeValueMemberN{Value: "3"},
			},
			ConditionExpression:       ptrStr("version = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: "1"}},
		})
		require.Error(t, err)
	})
}

// =============================================================================
// GSI Operations
// =============================================================================

func TestStore_GSI(t *testing.T) {
	t.Run("item inserted to GSI", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#john@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
			"name":   &types.AttributeValueMemberS{Value: "John"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		// Query main table
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#123"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, item, got.Item)

		// Query GSI
		gsiName := "gsi1"
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#john@example.com"},
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		assert.Equal(t, item, result.Items[0])
	})

	t.Run("GSI updated when keys change", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item1 := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#old@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item1,
		})
		require.NoError(t, err)

		item2 := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#new@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item2,
		})
		require.NoError(t, err)

		gsiName := "gsi1"

		// Old GSI key should not find anything
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#old@example.com"},
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
				":pk": &types.AttributeValueMemberS{Value: "email#new@example.com"},
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
	})

	t.Run("GSI entry deleted when main item deleted", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		item := map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "user#123"},
			"sk":     &types.AttributeValueMemberS{Value: "profile"},
			"gsi1pk": &types.AttributeValueMemberS{Value: "email#test@example.com"},
			"gsi1sk": &types.AttributeValueMemberS{Value: "user"},
		}

		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item:      item,
		})
		require.NoError(t, err)

		_, err = store.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "user#123"},
				"sk": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		require.NoError(t, err)

		gsiName := "gsi1"
		result, err := store.Query(ctx, &dynamodb.QueryInput{
			TableName:              &singleTableDesign.Name,
			IndexName:              &gsiName,
			KeyConditionExpression: ptrStr("gsi1pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "email#test@example.com"},
			},
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 0)
	})
}

// =============================================================================
// Query Operations
// =============================================================================

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

// =============================================================================
// Scan Operations
// =============================================================================

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

// =============================================================================
// Batch Operations
// =============================================================================

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

// =============================================================================
// Transaction Operations
// =============================================================================

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
		_, err = store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "new#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.Error(t, err) // Should not exist
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
}

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

// =============================================================================
// Key Encoding Tests
// =============================================================================

func TestKeyEncoding_NumberOrdering(t *testing.T) {
	store := newTestStore(t, numericSortKeyTable)
	ctx := context.Background()

	// Insert items with various numeric values including negative
	values := []string{"-100", "-10", "-1", "0", "1", "10", "100", "1000"}
	for _, v := range values {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &numericSortKeyTable.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberN{Value: v},
			},
		})
		require.NoError(t, err)
	}

	// Query and verify ordering
	result, err := store.Query(ctx, &dynamodb.QueryInput{
		TableName:              &numericSortKeyTable.Name,
		KeyConditionExpression: ptrStr("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Items, len(values))

	// Verify items are in ascending order
	for i, item := range result.Items {
		assert.Equal(t, values[i], item["sk"].(*types.AttributeValueMemberN).Value)
	}
}

func TestKeyEncoding_StringOrdering(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Insert items with various string values
	values := []string{"a", "aa", "ab", "b", "ba", "bb"}
	for _, v := range values {
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "test"},
				"sk": &types.AttributeValueMemberS{Value: v},
			},
		})
		require.NoError(t, err)
	}

	// Query and verify ordering
	result, err := store.Query(ctx, &dynamodb.QueryInput{
		TableName:              &singleTableDesign.Name,
		KeyConditionExpression: ptrStr("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Items, len(values))

	// Verify items are in ascending order
	for i, item := range result.Items {
		assert.Equal(t, values[i], item["sk"].(*types.AttributeValueMemberS).Value)
	}
}

// =============================================================================
// Data Type Tests
// =============================================================================

func TestStore_AllDataTypes(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "test"},
		"sk":         &types.AttributeValueMemberS{Value: "test"},
		"string":     &types.AttributeValueMemberS{Value: "hello"},
		"number":     &types.AttributeValueMemberN{Value: "123.45"},
		"binary":     &types.AttributeValueMemberB{Value: []byte("binary data")},
		"bool_true":  &types.AttributeValueMemberBOOL{Value: true},
		"bool_false": &types.AttributeValueMemberBOOL{Value: false},
		"null":       &types.AttributeValueMemberNULL{Value: true},
		"string_set": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
		"number_set": &types.AttributeValueMemberNS{Value: []string{"1", "2", "3"}},
		"binary_set": &types.AttributeValueMemberBS{Value: [][]byte{[]byte("a"), []byte("b")}},
		"list": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "item1"},
			&types.AttributeValueMemberN{Value: "42"},
		}},
		"map": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"nested_string": &types.AttributeValueMemberS{Value: "nested"},
			"nested_number": &types.AttributeValueMemberN{Value: "99"},
		}},
	}

	_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &singleTableDesign.Name,
		Item:      item,
	})
	require.NoError(t, err)

	got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &singleTableDesign.Name,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test"},
			"sk": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	require.NoError(t, err)

	// Verify each type
	assert.Equal(t, "hello", got.Item["string"].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, "123.45", got.Item["number"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, []byte("binary data"), got.Item["binary"].(*types.AttributeValueMemberB).Value)
	assert.True(t, got.Item["bool_true"].(*types.AttributeValueMemberBOOL).Value)
	assert.False(t, got.Item["bool_false"].(*types.AttributeValueMemberBOOL).Value)
	assert.True(t, got.Item["null"].(*types.AttributeValueMemberNULL).Value)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got.Item["string_set"].(*types.AttributeValueMemberSS).Value)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, got.Item["number_set"].(*types.AttributeValueMemberNS).Value)

	list := got.Item["list"].(*types.AttributeValueMemberL).Value
	assert.Len(t, list, 2)

	m := got.Item["map"].(*types.AttributeValueMemberM).Value
	assert.Equal(t, "nested", m["nested_string"].(*types.AttributeValueMemberS).Value)
}
