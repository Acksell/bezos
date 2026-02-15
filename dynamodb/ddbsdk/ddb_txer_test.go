package ddbsdk

import (
	"context"
	"testing"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

var txTestTable = table.TableDefinition{
	Name: "tx-test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

func txTestKey(pk, sk string) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: txTestTable.KeyDefinitions,
		Values:     table.PrimaryKeyValues{PartitionKey: pk, SortKey: sk},
	}
}

func TestTransaction_SinglePut(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	tx := db.NewTx()

	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
		Age:  30,
	}
	pk := txTestKey(entity.PK, entity.SK)

	put := NewUnsafePut(txTestTable, pk, entity)
	tx.AddAction(put)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit failed: %v", err)
	}

	// Verify item was stored
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item == nil {
		t.Fatal("expected item to exist")
	}
}

func TestTransaction_MultiplePuts(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	tx := db.NewTx()

	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Age: 25},
		{PK: "user#3", SK: "profile", Name: "Charlie", Age: 35},
	}

	for _, item := range items {
		pk := txTestKey(item.PK, item.SK)
		put := NewUnsafePut(txTestTable, pk, &item)
		tx.AddAction(put)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit failed: %v", err)
	}

	// Verify all items were stored
	getter := db.NewLookup()
	for _, item := range items {
		pk := txTestKey(item.PK, item.SK)
		retrieved, err := getter.GetItem(ctx, GetItemRequest{
			Table: txTestTable,
			Key:   pk,
		})
		if err != nil {
			t.Fatalf("GetItem failed: %v", err)
		}
		if retrieved == nil {
			t.Errorf("expected item %s/%s to exist", item.PK, item.SK)
		}
	}
}

func TestTransaction_PutAndDelete(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create an existing item
	existing := &testEntity{PK: "user#1", SK: "profile", Name: "Alice"}
	existingPK := txTestKey(existing.PK, existing.SK)
	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, existingPK, existing)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Transaction: delete existing, add new
	tx := db.NewTx()

	newEntity := &testEntity{PK: "user#2", SK: "profile", Name: "Bob"}
	newPK := txTestKey(newEntity.PK, newEntity.SK)

	tx.AddAction(NewDelete(txTestTable, existingPK))
	tx.AddAction(NewUnsafePut(txTestTable, newPK, newEntity))

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit failed: %v", err)
	}

	// Verify results
	getter := db.NewLookup()

	// Old item should be deleted
	item1, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   existingPK,
	})
	// ddbstore returns error for not found
	if err != nil {
		if item1 != nil {
			t.Errorf("expected item to be nil when error occurs, got: %v", item1)
		}
		// Verify it's a not found error
		if !contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' error, got: %v", err)
		}
	} else if item1 != nil {
		t.Error("expected old item to be deleted")
	}

	// New item should exist
	item2, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   newPK,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item2 == nil {
		t.Error("expected new item to exist")
	}
}

func TestTransaction_Update(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create an existing item
	existing := &testEntity{PK: "user#1", SK: "profile", Name: "Alice", Age: 30}
	pk := txTestKey(existing.PK, existing.SK)
	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, pk, existing)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Transaction with update
	tx := db.NewTx()
	update := NewUnsafeUpdate(txTestTable, pk).
		AddOp(SetFieldOp("name", "Alice Updated")).
		AddOp(SetFieldOp("age", 31))
	tx.AddAction(update)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit failed: %v", err)
	}

	// Verify update
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if retrieved.Name != "Alice Updated" || retrieved.Age != 31 {
		t.Errorf("expected Name='Alice Updated' and Age=31, got Name=%q Age=%d", retrieved.Name, retrieved.Age)
	}
}

func TestTransaction_WithConditions(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create an existing item
	existing := &testEntity{PK: "user#1", SK: "profile", Name: "Alice", Active: true}
	pk := txTestKey(existing.PK, existing.SK)
	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, pk, existing)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Transaction with condition
	tx := db.NewTx()

	cond := expression.Equal(expression.Name("active"), expression.Value(true))
	update := NewUnsafeUpdate(txTestTable, pk).
		AddOp(SetFieldOp("name", "Alice Updated")).
		WithCondition(cond)
	tx.AddAction(update)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit should succeed: %v", err)
	}

	// Verify update
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if retrieved.Name != "Alice Updated" {
		t.Errorf("expected Name='Alice Updated', got %q", retrieved.Name)
	}
}

func TestTransaction_ConditionFails(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create an existing item
	existing := &testEntity{PK: "user#1", SK: "profile", Name: "Alice", Active: false}
	pk := txTestKey(existing.PK, existing.SK)
	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, pk, existing)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Transaction with failing condition
	tx := db.NewTx()

	cond := expression.Equal(expression.Name("active"), expression.Value(true))
	update := NewUnsafeUpdate(txTestTable, pk).
		AddOp(SetFieldOp("name", "Alice Updated")).
		WithCondition(cond)
	tx.AddAction(update)

	// Should fail because active=false
	err := tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected transaction to fail due to condition")
	}

	// Verify item was not updated
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: txTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if retrieved.Name != "Alice" {
		t.Errorf("expected Name='Alice' (unchanged), got %q", retrieved.Name)
	}
}

func TestTransaction_DuplicateAction_Fails(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	tx := db.NewTx()

	entity := &testEntity{PK: "user#1", SK: "profile", Name: "Alice"}
	pk := txTestKey(entity.PK, entity.SK)

	// Add same action twice
	put1 := NewUnsafePut(txTestTable, pk, entity)
	put2 := NewUnsafePut(txTestTable, pk, entity)

	tx.AddAction(put1)
	tx.AddAction(put2)

	// Should fail due to duplicate
	err := tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected error for duplicate action")
	}
}

func TestTransaction_EmptyTransaction(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	tx := db.NewTx()

	// Commit empty transaction
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Empty transaction should succeed: %v", err)
	}
}

func TestTransaction_MixedOperations(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Setup: create some existing items
	existing1 := &testEntity{PK: "user#1", SK: "profile", Name: "Alice", Age: 30}
	existing2 := &testEntity{PK: "user#2", SK: "profile", Name: "Bob", Age: 25}

	pk1 := txTestKey(existing1.PK, existing1.SK)
	pk2 := txTestKey(existing2.PK, existing2.SK)

	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, pk1, existing1)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}
	if err := db.PutItem(ctx, NewUnsafePut(txTestTable, pk2, existing2)); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Transaction with mixed operations
	tx := db.NewTx()

	// Update user#1
	update := NewUnsafeUpdate(txTestTable, pk1).AddOp(SetFieldOp("age", 31))
	tx.AddAction(update)

	// Delete user#2
	del := NewDelete(txTestTable, pk2)
	tx.AddAction(del)

	// Add user#3
	newEntity := &testEntity{PK: "user#3", SK: "profile", Name: "Charlie", Age: 35}
	newPK := txTestKey(newEntity.PK, newEntity.SK)
	put := NewUnsafePut(txTestTable, newPK, newEntity)
	tx.AddAction(put)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Transaction commit failed: %v", err)
	}

	// Verify all operations
	getter := db.NewLookup()

	// user#1 should be updated
	item1, err := getter.GetItem(ctx, GetItemRequest{Table: txTestTable, Key: pk1})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	var retrieved1 testEntity
	if err := attributevalue.UnmarshalMap(item1, &retrieved1); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if retrieved1.Age != 31 {
		t.Errorf("expected Age=31, got %d", retrieved1.Age)
	}

	// user#2 should be deleted
	item2, err := getter.GetItem(ctx, GetItemRequest{Table: txTestTable, Key: pk2})
	// ddbstore returns error for not found
	if err != nil {
		if item2 != nil {
			t.Errorf("expected item to be nil when error occurs, got: %v", item2)
		}
		// Verify it's a not found error
		if !contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' error, got: %v", err)
		}
	} else if item2 != nil {
		t.Error("expected user#2 to be deleted")
	}

	// user#3 should exist
	item3, err := getter.GetItem(ctx, GetItemRequest{Table: txTestTable, Key: newPK})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item3 == nil {
		t.Error("expected user#3 to exist")
	}
}

func TestTransaction_GetItemsTx(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Age: 25},
		{PK: "user#3", SK: "profile", Name: "Charlie", Age: 35},
	}

	for _, item := range items {
		pk := txTestKey(item.PK, item.SK)
		put := NewUnsafePut(txTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Get items transactionally
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{Table: txTestTable, Key: txTestKey("user#1", "profile")},
		{Table: txTestTable, Key: txTestKey("user#2", "profile")},
		{Table: txTestTable, Key: txTestKey("user#3", "profile")},
	}

	results, err := getter.GetItemsTx(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsTx failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 items, got %d", len(results))
	}

	// Verify items
	names := make(map[string]bool)
	for _, item := range results {
		var entity testEntity
		if err := attributevalue.UnmarshalMap(item, &entity); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		names[entity.Name] = true
	}

	expectedNames := []string{"Alice", "Bob", "Charlie"}
	for _, name := range expectedNames {
		if !names[name] {
			t.Errorf("expected to find %s in results", name)
		}
	}
}

func TestTransaction_GetItemsTx_WithProjection(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Create test items
	items := []testEntity{
		{PK: "user#1", SK: "profile", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "user#2", SK: "profile", Name: "Bob", Email: "bob@example.com", Age: 25},
	}

	for _, item := range items {
		pk := txTestKey(item.PK, item.SK)
		put := NewUnsafePut(txTestTable, pk, &item)
		if err := db.PutItem(ctx, put); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Get with projection
	getter := db.NewLookup()
	requests := []GetItemRequest{
		{
			Table:      txTestTable,
			Key:        txTestKey("user#1", "profile"),
			Projection: []string{"pk", "sk", "name"},
		},
		{
			Table:      txTestTable,
			Key:        txTestKey("user#2", "profile"),
			Projection: []string{"pk", "sk", "name"},
		},
	}

	results, err := getter.GetItemsTx(ctx, requests...)
	if err != nil {
		t.Fatalf("GetItemsTx failed: %v", err)
	}

	// Verify only projected fields are present
	for _, item := range results {
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
}

func TestTransaction_GetItemsTx_TooManyItems(t *testing.T) {
	db := NewMemoryClient(txTestTable)
	ctx := context.Background()

	// Try to get more than 100 items
	getter := db.NewLookup()
	requests := make([]GetItemRequest, 101)
	for i := 0; i < 101; i++ {
		requests[i] = GetItemRequest{
			Table: txTestTable,
			Key:   txTestKey("user#1", "profile"),
		}
	}

	_, err := getter.GetItemsTx(ctx, requests...)
	if err == nil {
		t.Fatal("expected error for >100 items in transaction")
	}
}
