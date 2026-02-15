package ddbsdk

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func TestClient_DeleteItem_Basic(t *testing.T) {
	db := NewMemoryClient(clientTestTable)
	ctx := context.Background()

	// First create an item
	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Delete the item
	del := NewDelete(clientTestTable, pk)
	if err := db.DeleteItem(ctx, del); err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}

	// Verify item is gone
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})

	// ddbstore returns an error for not found
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
		t.Error("expected item to be deleted")
	}
}

func TestClient_DeleteItem_WithCondition(t *testing.T) {
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

	// Delete with condition that item is active
	cond := expression.Equal(expression.Name("active"), expression.Value(true))
	del := NewDelete(clientTestTable, pk).WithCondition(cond)
	if err := db.DeleteItem(ctx, del); err != nil {
		t.Fatalf("DeleteItem with matching condition should succeed: %v", err)
	}

	// Verify item is gone
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})

	// ddbstore returns an error for not found
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
		t.Error("expected item to be deleted")
	}
}

func TestClient_DeleteItem_ConditionFails(t *testing.T) {
	db := NewMemoryClient(clientTestTable)
	ctx := context.Background()

	// Create an item
	entity := &testEntity{
		PK:     "user#1",
		SK:     "profile",
		Name:   "Alice",
		Active: false,
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Delete with condition that item is active (should fail)
	cond := expression.Equal(expression.Name("active"), expression.Value(true))
	del := NewDelete(clientTestTable, pk).WithCondition(cond)
	err := db.DeleteItem(ctx, del)
	if err == nil {
		t.Fatal("expected error when condition doesn't match")
	}

	// Verify item still exists
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item == nil {
		t.Error("expected item to still exist")
	}
}
