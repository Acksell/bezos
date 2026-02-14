package ddbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
