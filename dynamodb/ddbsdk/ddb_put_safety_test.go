package ddbsdk

import (
	"context"
	"testing"

	"github.com/acksell/bezos/dynamodb/table"
)

type versionedEntity struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
	Name string `dynamodbav:"name"`
	Ver  int    `dynamodbav:"version"`
}

func (e *versionedEntity) IsValid() error { return nil }

func (e *versionedEntity) VersionField() (string, any) {
	return "version", e.Ver
}

var testTable = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

func testPrimaryKey(pk, sk string) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: testTable.KeyDefinitions,
		Values:     table.PrimaryKeyValues{PartitionKey: pk, SortKey: sk},
	}
}

func TestSafePut_NewItem_Succeeds(t *testing.T) {
	db := NewMemoryClient(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// nil old means conditional create
	put := NewSafePut(testTable, pk, nil, entity)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("expected no error for new item, got: %v", err)
	}
}

func TestSafePut_CreateFailsIfItemExists(t *testing.T) {
	db := NewMemoryClient(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// First create succeeds
	put := NewSafePut(testTable, pk, nil, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}

	// Second create with nil old should fail (item already exists)
	entity2 := &versionedEntity{PK: "user#1", SK: "profile", Name: "Bob", Ver: 1}
	put = NewSafePut(testTable, pk, nil, entity2)
	err := db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("expected error when creating item that already exists, got nil")
	}
}

func TestSafePut_CorrectOldVersion_Succeeds(t *testing.T) {
	db := NewMemoryClient(testTable)
	ctx := context.Background()

	old := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(old.PK, old.SK)

	// Insert initial item
	put := NewSafePut(testTable, pk, nil, old)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("initial create should succeed: %v", err)
	}

	// Update with correct old version
	new := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice Updated", Ver: 2}
	put = NewSafePut(testTable, pk, old, new)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("expected update with correct old version to succeed, got: %v", err)
	}
}

func TestSafePut_WrongOldVersion_Fails(t *testing.T) {
	db := NewMemoryClient(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// Insert initial item
	put := NewSafePut(testTable, pk, nil, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("initial create should succeed: %v", err)
	}

	// Try to update with wrong old version
	wrongOld := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 99}
	new := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice Updated", Ver: 2}
	put = NewSafePut(testTable, pk, wrongOld, new)
	err := db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("expected error when old version doesn't match, got nil")
	}
}

func TestSafePut_ConcurrentWriters_OnlyFirstSucceeds(t *testing.T) {
	db := NewMemoryClient(testTable)
	ctx := context.Background()

	// Both writers read the same initial state
	initial := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(initial.PK, initial.SK)

	// Insert initial item
	put := NewSafePut(testTable, pk, nil, initial)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("initial create should succeed: %v", err)
	}

	// Both readers got version 1
	oldFromReader1 := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	oldFromReader2 := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}

	// Writer 1 updates to version 2
	new1 := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice by Writer1", Ver: 2}
	put = NewSafePut(testTable, pk, oldFromReader1, new1)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first writer should succeed: %v", err)
	}

	// Writer 2 also tries to update to version 2, using stale old version
	new2 := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice by Writer2", Ver: 2}
	put = NewSafePut(testTable, pk, oldFromReader2, new2)
	err := db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("second writer should fail because version in DB is now 2, not 1")
	}
}
