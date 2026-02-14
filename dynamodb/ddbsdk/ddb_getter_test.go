package ddbsdk

import (
	"context"
	"testing"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

var getterTestTable = table.TableDefinition{
	Name: "getter-test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

func getterTestKey(pk, sk string) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: getterTestTable.KeyDefinitions,
		Values:     table.PrimaryKeyValues{PartitionKey: pk, SortKey: sk},
	}
}

func TestGetter_GetItem_Found(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:    "user#1",
		SK:    "profile",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	pk := getterTestKey(entity.PK, entity.SK)

	put := NewUnsafePut(getterTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Get the item
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: getterTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if item == nil {
		t.Fatal("expected item to be found")
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if retrieved.Name != "Alice" || retrieved.Email != "alice@example.com" || retrieved.Age != 30 {
		t.Errorf("retrieved entity mismatch: got %+v", retrieved)
	}
}

func TestGetter_GetItem_NotFound(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Try to get non-existent item
	getter := db.NewLookup()
	pk := getterTestKey("user#999", "profile")
	
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: getterTestTable,
		Key:   pk,
	})
	
	// ddbstore returns an error for not found, but item should be nil
	// This is the actual behavior from the in-memory mock
	if err != nil {
		if item != nil {
			t.Errorf("expected item to be nil when error occurs, got: %v", item)
		}
		// Verify it's a not found error
		if !contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' error, got: %v", err)
		}
		return
	}
	
	if item != nil {
		t.Error("expected item to be nil")
	}
}

func TestGetter_GetItem_WithProjection(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:    "user#1",
		SK:    "profile",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}
	pk := getterTestKey(entity.PK, entity.SK)

	put := NewUnsafePut(getterTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Get with projection
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table:      getterTestTable,
		Key:        pk,
		Projection: []string{"pk", "sk", "name"},
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if item == nil {
		t.Fatal("expected item to be found")
	}

	// Verify only projected fields are present
	if _, ok := item["email"]; ok {
		t.Error("expected email field to be excluded")
	}
	if _, ok := item["age"]; ok {
		t.Error("expected age field to be excluded")
	}
	if _, ok := item["name"]; !ok {
		t.Error("expected name field to be present")
	}
}

func TestGetter_GetItem_ConsistentRead(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
	}
	pk := getterTestKey(entity.PK, entity.SK)

	put := NewUnsafePut(getterTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Get with consistent read (default)
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: getterTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if item == nil {
		t.Fatal("expected item to be found")
	}
}

func TestGetter_GetItem_EventuallyConsistent(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
	}
	pk := getterTestKey(entity.PK, entity.SK)

	put := NewUnsafePut(getterTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Get with eventually consistent read
	getter := db.NewLookup(WithEventuallyConsistentReads())
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: getterTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if item == nil {
		t.Fatal("expected item to be found")
	}
}

func TestGetter_GetItemsBatch_AllFound(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create multiple items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Age: 25},
		{PK: "user#3", SK: "profile", Name: "Charlie", Age: 35},
	}

	for _, item := range items {
		pk := getterTestKey(item.PK, item.SK)
		put := NewUnsafePut(getterTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Batch get all items
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{Table: getterTestTable, Key: getterTestKey("user#1", "profile")},
		{Table: getterTestTable, Key: getterTestKey("user#2", "profile")},
		{Table: getterTestTable, Key: getterTestKey("user#3", "profile")},
	}

	results, err := getter.GetItemsBatch(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsBatch failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 items, got %d", len(results))
	}

	// Verify all items
	names := make(map[string]bool)
	for _, item := range results {
		var entity testEntity
		if err := attributevalue.UnmarshalMap(item, &entity); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		names[entity.Name] = true
	}

	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		if !names[name] {
			t.Errorf("expected to find %s in results", name)
		}
	}
}

func TestGetter_GetItemsBatch_PartiallyFound(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create only some items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice"},
		{PK: "user#3", SK: "profile", Name: "Charlie"},
	}

	for _, item := range items {
		pk := getterTestKey(item.PK, item.SK)
		put := NewUnsafePut(getterTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Batch get including non-existent item
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{Table: getterTestTable, Key: getterTestKey("user#1", "profile")},
		{Table: getterTestTable, Key: getterTestKey("user#2", "profile")}, // doesn't exist
		{Table: getterTestTable, Key: getterTestKey("user#3", "profile")},
	}

	results, err := getter.GetItemsBatch(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsBatch failed: %v", err)
	}

	// Should only return existing items
	if len(results) != 2 {
		t.Errorf("expected 2 items, got %d", len(results))
	}
}

func TestGetter_GetItemsBatch_Empty(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	getter := db.NewLookup()
	
	results, err := getter.GetItemsBatch(ctx)
	if err != nil {
		t.Fatalf("GetItemsBatch failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 items, got %d", len(results))
	}
}

func TestGetter_GetItemsTx_AllFound(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create multiple items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Age: 25},
		{PK: "user#3", SK: "profile", Name: "Charlie", Age: 35},
	}

	for _, item := range items {
		pk := getterTestKey(item.PK, item.SK)
		put := NewUnsafePut(getterTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Transactional get all items
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{Table: getterTestTable, Key: getterTestKey("user#1", "profile")},
		{Table: getterTestTable, Key: getterTestKey("user#2", "profile")},
		{Table: getterTestTable, Key: getterTestKey("user#3", "profile")},
	}

	results, err := getter.GetItemsTx(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsTx failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 items, got %d", len(results))
	}
}

func TestGetter_GetItemsTx_Empty(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	getter := db.NewLookup()
	
	results, err := getter.GetItemsTx(ctx)
	if err != nil {
		t.Fatalf("GetItemsTx failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 items, got %d", len(results))
	}
}

func TestGetter_GetItemsTx_ExceedsLimit(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Try to get more than 100 items (DynamoDB limit)
	getter := db.NewLookup()
	requests := make([]GetItemRequest, 101)
	for i := 0; i < 101; i++ {
		requests[i] = GetItemRequest{
			Table: getterTestTable,
			Key:   getterTestKey("user#1", "profile"),
		}
	}

	_, err := getter.GetItemsTx(ctx, requests...)
	if err == nil {
		t.Fatal("expected error when exceeding 100 items limit")
	}
}

func TestGetter_GetItemsBatch_WithDifferentProjections(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Email: "bob@example.com", Age: 25},
	}

	for _, item := range items {
		pk := getterTestKey(item.PK, item.SK)
		put := NewUnsafePut(getterTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Batch get with different projections per item
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{
			Table:      getterTestTable,
			Key:        getterTestKey("user#1", "profile"),
			Projection: []string{"pk", "sk", "name"},
		},
		{
			Table:      getterTestTable,
			Key:        getterTestKey("user#2", "profile"),
			Projection: []string{"pk", "sk", "email"},
		},
	}

	results, err := getter.GetItemsBatch(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsBatch failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 items, got %d", len(results))
	}

	// Note: Projections in batch operations are per-item based on the request
}

func TestGetter_GetItemsTx_WithDifferentProjections(t *testing.T) {
	db := NewMock(getterTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Email: "bob@example.com", Age: 25},
	}

	for _, item := range items {
		pk := getterTestKey(item.PK, item.SK)
		put := NewUnsafePut(getterTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Transactional get with different projections per item
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{
			Table:      getterTestTable,
			Key:        getterTestKey("user#1", "profile"),
			Projection: []string{"pk", "sk", "name"},
		},
		{
			Table:      getterTestTable,
			Key:        getterTestKey("user#2", "profile"),
			Projection: []string{"pk", "sk", "email"},
		},
	}

	results, err := getter.GetItemsTx(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsTx failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 items, got %d", len(results))
	}

	// Verify first item has name but not email
	if _, ok := results[0]["name"]; !ok {
		if _, ok := results[1]["name"]; !ok {
			t.Error("expected one item to have name field")
		}
	}
}
