package ddbsdk

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func TestClient_PutItem_Basic(t *testing.T) {
	db := NewMock(clientTestTable)
	ctx := context.Background()

	entity := &testEntity{
		PK:     "user#1",
		SK:     "profile",
		Name:   "Alice",
		Email:  "alice@example.com",
		Age:    30,
		Active: true,
	}
	pk := testKey(entity.PK, entity.SK)

	put := NewUnsafePut(clientTestTable, pk, entity)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Verify item was stored
	getter := db.NewLookup()
	item, err := getter.GetItem(ctx, GetItemRequest{
		Table: clientTestTable,
		Key:   pk,
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item == nil {
		t.Fatal("expected item to exist")
	}

	var retrieved testEntity
	if err := attributevalue.UnmarshalMap(item, &retrieved); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if retrieved.Name != entity.Name || retrieved.Email != entity.Email {
		t.Errorf("retrieved entity mismatch: got %+v, want %+v", retrieved, entity)
	}
}

func TestClient_PutItem_WithTTL(t *testing.T) {
	db := NewMock(clientTestTable)
	ctx := context.Background()

	entity := &testEntity{
		PK:   "user#1",
		SK:   "session",
		Name: "Session1",
	}
	pk := testKey(entity.PK, entity.SK)
	expiry := time.Now().Add(24 * time.Hour)

	put := NewUnsafePut(clientTestTable, pk, entity).WithTTL(expiry)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("PutItem with TTL failed: %v", err)
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
	if item == nil {
		t.Fatal("expected item to exist")
	}

	if _, ok := item["ttl"]; !ok {
		t.Error("expected TTL field to be set")
	}
}

func TestClient_PutItem_WithCondition(t *testing.T) {
	db := NewMock(clientTestTable)
	ctx := context.Background()

	entity := &testEntity{
		PK:   "user#1",
		SK:   "profile",
		Name: "Alice",
	}
	pk := testKey(entity.PK, entity.SK)

	// First put with condition that item doesn't exist
	cond := expression.AttributeNotExists(expression.Name("pk"))
	put := NewUnsafePut(clientTestTable, pk, entity).WithCondition(cond)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("first PutItem should succeed: %v", err)
	}

	// Second put with same condition should fail
	entity.Name = "Alice Updated"
	put = NewUnsafePut(clientTestTable, pk, entity).WithCondition(cond)
	err = db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("expected error when putting item that already exists")
	}
}

func TestClient_SafePut_OptimisticLocking(t *testing.T) {
	db := NewMock(clientTestTable)
	ctx := context.Background()

	entity := &testEntity{
		PK:      "user#1",
		SK:      "profile",
		Name:    "Alice",
		Version: 1,
	}
	pk := testKey(entity.PK, entity.SK)

	// First put succeeds
	put := NewSafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first put should succeed: %v", err)
	}

	// Put with higher version succeeds
	entity.Version = 2
	entity.Name = "Alice Updated"
	put = NewSafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("put with higher version should succeed: %v", err)
	}

	// Put with same or lower version fails
	entity.Version = 2
	entity.Name = "Alice Updated Again"
	put = NewSafePut(clientTestTable, pk, entity)
	if err := db.PutItem(ctx, put); err == nil {
		t.Fatal("expected error when putting same version")
	}
}
