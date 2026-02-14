package ddbsdk

import (
	"strings"

	"github.com/acksell/bezos/dynamodb/table"
)

// testEntity is a shared test entity used across client operation tests
type testEntity struct {
	PK      string `dynamodbav:"pk"`
	SK      string `dynamodbav:"sk"`
	Name    string `dynamodbav:"name"`
	Email   string `dynamodbav:"email"`
	Age     int    `dynamodbav:"age"`
	Active  bool   `dynamodbav:"active"`
	Version int    `dynamodbav:"version"`
}

func (e *testEntity) IsValid() error { return nil }

func (e *testEntity) VersionField() (string, any) {
	return "version", e.Version
}

// clientTestTable is a shared table definition used across client operation tests
var clientTestTable = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	TimeToLiveKey: "ttl",
	GSIs: []table.GSIDefinition{
		{
			Name: "email-index",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

// testKey is a helper function to create a primary key for tests
func testKey(pk, sk string) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: clientTestTable.KeyDefinitions,
		Values:     table.PrimaryKeyValues{PartitionKey: pk, SortKey: sk},
	}
}

// contains is a helper function to check if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
