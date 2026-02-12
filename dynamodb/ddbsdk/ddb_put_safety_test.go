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

func (e *versionedEntity) Version() (string, any) {
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
	db := NewMock(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	put := NewSafePut(testTable, pk, entity)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("expected no error for new item, got: %v", err)
	}
}

func TestSafePut_SameVersion_Fails(t *testing.T) {
	db := NewMock(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// First put succeeds
	put := NewSafePut(testTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first put should succeed: %v", err)
	}

	// Same version should fail
	put = NewSafePut(testTable, pk, entity)
	err := db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("expected error when putting same version, got nil")
	}
}

func TestSafePut_LowerVersion_Fails(t *testing.T) {
	db := NewMock(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 2}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// Put version 2
	put := NewSafePut(testTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first put should succeed: %v", err)
	}

	// Try to put version 1 (lower)
	entity.Ver = 1
	put = NewSafePut(testTable, pk, entity)
	err := db.PutItem(ctx, put)
	if err == nil {
		t.Fatal("expected error when putting lower version, got nil")
	}
}

func TestSafePut_HigherVersion_Succeeds(t *testing.T) {
	db := NewMock(testTable)
	ctx := context.Background()

	entity := &versionedEntity{PK: "user#1", SK: "profile", Name: "Alice", Ver: 1}
	pk := testPrimaryKey(entity.PK, entity.SK)

	// Put version 1
	put := NewSafePut(testTable, pk, entity)
	if err := db.PutItem(ctx, put); err != nil {
		t.Fatalf("first put should succeed: %v", err)
	}

	// Put version 2 (higher)
	entity.Ver = 2
	entity.Name = "Alice Updated"
	put = NewSafePut(testTable, pk, entity)
	err := db.PutItem(ctx, put)
	if err != nil {
		t.Fatalf("expected higher version to succeed, got: %v", err)
	}
}
