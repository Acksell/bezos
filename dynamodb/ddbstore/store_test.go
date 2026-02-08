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
// UpdateItem Operations
// =============================================================================

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

	t.Run("update in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create items to update
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-update#1"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "10"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-update#2"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "20"},
			},
		})
		require.NoError(t, err)

		// Transaction with updates
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-update#1"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count + :inc"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":inc": &types.AttributeValueMemberN{Value: "5"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-update#2"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count - :dec"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":dec": &types.AttributeValueMemberN{Value: "3"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify both updates applied
		got1, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-update#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "15", got1.Item["count"].(*types.AttributeValueMemberN).Value)

		got2, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-update#2"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "17", got2.Item["count"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("update with condition in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-cond-update"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		// Transaction with conditional update - should succeed
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-cond-update"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression:    ptrStr("SET version = :newVer"),
						ConditionExpression: ptrStr("version = :oldVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":oldVer": &types.AttributeValueMemberN{Value: "1"},
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify update applied
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-cond-update"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "2", got.Item["version"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("update condition failure rolls back transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create items
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-rollback#1"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "tx-rollback#2"},
				"sk":      &types.AttributeValueMemberS{Value: "item"},
				"version": &types.AttributeValueMemberN{Value: "5"}, // Wrong version for condition
			},
		})
		require.NoError(t, err)

		// Transaction with update that will fail condition
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-rollback#1"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET version = :newVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-rollback#2"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression:    ptrStr("SET version = :newVer"),
						ConditionExpression: ptrStr("version = :oldVer"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":oldVer": &types.AttributeValueMemberN{Value: "1"}, // Will fail - actual is 5
							":newVer": &types.AttributeValueMemberN{Value: "2"},
						},
					},
				},
			},
		})
		require.Error(t, err)

		// Verify first update was NOT applied due to rollback
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-rollback#1"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "1", got.Item["version"].(*types.AttributeValueMemberN).Value) // Still 1, not 2
	})

	t.Run("update creates new item in transaction (upsert)", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Transaction with update on non-existent item
		_, err := store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-upsert"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #name = :name"),
						ExpressionAttributeNames: map[string]string{
							"#name": "name",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":name": &types.AttributeValueMemberS{Value: "created"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify item was created
		got, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-upsert"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "created", got.Item["name"].(*types.AttributeValueMemberS).Value)
		// Verify key attributes present
		assert.Equal(t, "tx-upsert", got.Item["pk"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "item", got.Item["sk"].(*types.AttributeValueMemberS).Value)
	})

	t.Run("mixed put update delete in transaction", func(t *testing.T) {
		store := newTestStore(t, singleTableDesign)
		ctx := context.Background()

		// Create item to update and delete
		_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "tx-mixed-update"},
				"sk":    &types.AttributeValueMemberS{Value: "item"},
				"count": &types.AttributeValueMemberN{Value: "100"},
			},
		})
		require.NoError(t, err)

		_, err = store.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &singleTableDesign.Name,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)

		// Mixed transaction
		_, err = store.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: &singleTableDesign.Name,
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "tx-mixed-put"},
							"sk":   &types.AttributeValueMemberS{Value: "item"},
							"data": &types.AttributeValueMemberS{Value: "new"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-mixed-update"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
						UpdateExpression: ptrStr("SET #count = #count + :inc"),
						ExpressionAttributeNames: map[string]string{
							"#count": "count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":inc": &types.AttributeValueMemberN{Value: "50"},
						},
					},
				},
				{
					Delete: &types.Delete{
						TableName: &singleTableDesign.Name,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
							"sk": &types.AttributeValueMemberS{Value: "item"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify put
		gotPut, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-put"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "new", gotPut.Item["data"].(*types.AttributeValueMemberS).Value)

		// Verify update
		gotUpdate, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-update"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "150", gotUpdate.Item["count"].(*types.AttributeValueMemberN).Value)

		// Verify delete
		_, err = store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tx-mixed-delete"},
				"sk": &types.AttributeValueMemberS{Value: "item"},
			},
		})
		require.Error(t, err) // Should not exist
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

// =============================================================================
// Projection Expression Tests
// =============================================================================

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

func TestStore_ProjectionExpression_ComplexNested(t *testing.T) {
	store := newTestStore(t, singleTableDesign)
	ctx := context.Background()

	// Create deeply nested item
	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "complex"},
		"sk": &types.AttributeValueMemberS{Value: "nested"},
		"user": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"profile": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"name": &types.AttributeValueMemberS{Value: "Deep Name"},
				"bio":  &types.AttributeValueMemberS{Value: "Deep Bio"},
			}},
			"settings": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"theme": &types.AttributeValueMemberS{Value: "dark"},
			}},
		}},
		"items": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: "a"},
				"name": &types.AttributeValueMemberS{Value: "Item A"},
			}},
			&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: "b"},
				"name": &types.AttributeValueMemberS{Value: "Item B"},
			}},
		}},
	}

	_, err := store.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &singleTableDesign.Name,
		Item:      item,
	})
	require.NoError(t, err)

	t.Run("deeply nested projection", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &singleTableDesign.Name,
			// todo, check if alias is needed even for nested fields that have reserved names.
			ProjectionExpression: ptrStr("#u.profile.#n"),
			ExpressionAttributeNames: map[string]string{
				"#u": "user",
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "complex"},
				"sk": &types.AttributeValueMemberS{Value: "nested"},
			},
		})
		require.NoError(t, err)

		expected := map[string]types.AttributeValue{
			"user": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"profile": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"name": &types.AttributeValueMemberS{Value: "Deep Name"},
				}},
			}},
		}
		assert.Equal(t, expected, result.Item)
	})

	t.Run("list item nested attribute", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#i[0].#n"),
			ExpressionAttributeNames: map[string]string{
				"#i": "items",
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "complex"},
				"sk": &types.AttributeValueMemberS{Value: "nested"},
			},
		})
		require.NoError(t, err)

		expected := map[string]types.AttributeValue{
			"items": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"name": &types.AttributeValueMemberS{Value: "Item A"},
				}},
			}},
		}
		assert.Equal(t, expected, result.Item)
	})

	t.Run("multiple paths merge correctly", func(t *testing.T) {
		result, err := store.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:            &singleTableDesign.Name,
			ProjectionExpression: ptrStr("#u.profile.#n, #u.settings.theme"),
			ExpressionAttributeNames: map[string]string{
				"#u": "user",
				"#n": "name",
			},
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "complex"},
				"sk": &types.AttributeValueMemberS{Value: "nested"},
			},
		})
		require.NoError(t, err)

		expected := map[string]types.AttributeValue{
			"user": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"profile": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"name": &types.AttributeValueMemberS{Value: "Deep Name"},
				}},
				"settings": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"theme": &types.AttributeValueMemberS{Value: "dark"},
				}},
			}},
		}
		assert.Equal(t, expected, result.Item)
	})
}
