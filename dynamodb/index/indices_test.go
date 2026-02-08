package index_test

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/keys"
	"github.com/acksell/bezos/dynamodb/table"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSecondaryIndex_ExtractKeys(t *testing.T) {
	gsi := index.SecondaryIndex{
		Name: "gsi1",
		PartitionKey: keys.Key{
			Def:       table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("EMAIL#%s", keys.Field("email")),
		},
		SortKey: &keys.Key{
			Def:       table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("USER#%s", keys.Field("userID")),
		},
	}

	item := map[string]types.AttributeValue{
		"userID": &types.AttributeValueMemberS{Value: "123"},
		"email":  &types.AttributeValueMemberS{Value: "test@example.com"},
		"name":   &types.AttributeValueMemberS{Value: "Test User"},
	}

	gsiKeys, err := gsi.ExtractKeys(item)
	if err != nil {
		t.Fatalf("ExtractKeys() error = %v", err)
	}

	if len(gsiKeys) != 2 {
		t.Errorf("ExtractKeys() returned %d keys, want 2", len(gsiKeys))
	}

	pk, ok := gsiKeys["gsi1pk"].(*types.AttributeValueMemberS)
	if !ok || pk.Value != "EMAIL#test@example.com" {
		t.Errorf("gsi1pk = %v, want EMAIL#test@example.com", gsiKeys["gsi1pk"])
	}

	sk, ok := gsiKeys["gsi1sk"].(*types.AttributeValueMemberS)
	if !ok || sk.Value != "USER#123" {
		t.Errorf("gsi1sk = %v, want USER#123", gsiKeys["gsi1sk"])
	}
}

func TestSecondaryIndex_SparseIndex(t *testing.T) {
	gsi := index.SecondaryIndex{
		Name: "gsi1",
		PartitionKey: keys.Key{
			Def:       table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("EMAIL#%s", keys.Field("email")),
		},
		SortKey: &keys.Key{
			Def:       table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("USER#%s", keys.Field("userID")),
		},
	}

	// Item missing the email field - should return nil (sparse GSI behavior)
	item := map[string]types.AttributeValue{
		"userID": &types.AttributeValueMemberS{Value: "123"},
		"name":   &types.AttributeValueMemberS{Value: "Test User"},
	}

	gsiKeys, err := gsi.ExtractKeys(item)
	if err != nil {
		t.Fatalf("ExtractKeys() error = %v", err)
	}

	if gsiKeys != nil {
		t.Errorf("ExtractKeys() = %v, want nil for sparse GSI", gsiKeys)
	}
}

func TestPrimaryIndex_ExtractAllGSIKeys(t *testing.T) {
	tbl := table.TableDefinition{
		Name: "TestTable",
		KeyDefinitions: table.PrimaryKeyDefinition{
			PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
			SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
		},
	}

	gsi1 := index.SecondaryIndex{
		Name: "gsi1",
		PartitionKey: keys.Key{
			Def:       table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("EMAIL#%s", keys.Field("email")),
		},
		SortKey: &keys.Key{
			Def:       table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			Extractor: keys.Const("USER"),
		},
	}

	gsi2 := index.SecondaryIndex{
		Name: "gsi2",
		PartitionKey: keys.Key{
			Def:       table.KeyDef{Name: "gsi2pk", Kind: table.KeyKindS},
			Extractor: keys.Fmt("STATUS#%s", keys.Field("status")),
		},
		SortKey: &keys.Key{
			Def:       table.KeyDef{Name: "gsi2sk", Kind: table.KeyKindS},
			Extractor: keys.Field("createdAt"),
		},
	}

	idx := index.PrimaryIndex{
		Table:        tbl,
		PartitionKey: keys.Fmt("USER#%s", keys.Field("userID")),
		SortKey:      keys.Const("PROFILE"),
		Secondary:    []index.SecondaryIndex{gsi1, gsi2},
	}

	item := map[string]types.AttributeValue{
		"userID":    &types.AttributeValueMemberS{Value: "123"},
		"email":     &types.AttributeValueMemberS{Value: "test@example.com"},
		"status":    &types.AttributeValueMemberS{Value: "active"},
		"createdAt": &types.AttributeValueMemberS{Value: "2024-01-01"},
	}

	gsiKeys, err := idx.ExtractAllGSIKeys(item)
	if err != nil {
		t.Fatalf("ExtractAllGSIKeys() error = %v", err)
	}

	// Should have 4 keys: gsi1pk, gsi1sk, gsi2pk, gsi2sk
	if len(gsiKeys) != 4 {
		t.Errorf("ExtractAllGSIKeys() returned %d keys, want 4", len(gsiKeys))
	}

	expectedKeys := map[string]string{
		"gsi1pk": "EMAIL#test@example.com",
		"gsi1sk": "USER",
		"gsi2pk": "STATUS#active",
		"gsi2sk": "2024-01-01",
	}

	for name, want := range expectedKeys {
		got, ok := gsiKeys[name].(*types.AttributeValueMemberS)
		if !ok {
			t.Errorf("key %q missing or wrong type", name)
			continue
		}
		if got.Value != want {
			t.Errorf("key %q = %q, want %q", name, got.Value, want)
		}
	}
}
