package ddbsdk

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func TestClient_UpdateItem_Basic(t *testing.T) {
	db := NewMemoryClient(clientTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
		Age:  30,
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Update the item
	update := NewUnsafeUpdate(clientTestTable, pk).
		AddOp(SetFieldOp("name", "Alice Updated")).
		AddOp(SetFieldOp("age", 31))

	if err := db.UpdateItem(ctx, update); err != nil {
		t.Fatalf("UpdateItem failed: %v", err)
	}

	// Verify update
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if retrieved.Name != "Alice Updated" || retrieved.Age != 31 {
		t.Errorf("expected Name='Alice Updated' and Age=31, got Name=%q Age=%d", retrieved.Name, retrieved.Age)
	}
}

func TestClient_UpdateItem_WithCondition(t *testing.T) {
	db := NewMemoryClient(clientTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:     "user#1",
		SK:     "profile",
		Name:   "Alice",
		Active: true,
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Update with condition
	cond := expression.Equal(expression.Name("active"), expression.Value(true))
	update := NewUnsafeUpdate(clientTestTable, pk).
		AddOp(SetFieldOp("name", "Alice Updated")).
		WithCondition(cond)

	if err := db.UpdateItem(ctx, update); err != nil {
		t.Fatalf("UpdateItem with matching condition should succeed: %v", err)
	}

	// Verify update
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if retrieved.Name != "Alice Updated" {
		t.Errorf("expected Name='Alice Updated', got %q", retrieved.Name)
	}
}

func TestClient_UpdateItem_RefreshTTL(t *testing.T) {
	db := NewMemoryClient(clientTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:   "user#1",
		SK:   "session",
		Name: "Session1",
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Update TTL
	expiry := time.Now().Add(24 * time.Hour)
	update := NewUnsafeUpdate(clientTestTable, pk).RefreshTTL(expiry)

	if err := db.UpdateItem(ctx, update); err != nil {
		t.Fatalf("UpdateItem with TTL failed: %v", err)
	}

	// Verify TTL was set
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if _, ok := item["ttl"]; !ok {
		t.Error("expected TTL field to be set")
	}
}
