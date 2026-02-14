package ddbstore

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/table"
	"github.com/stretchr/testify/require"
)

// Test table definitions
var singleTableDesign = table.TableDefinition{
	Name: "test-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	GSIs: []table.GSIDefinition{
		{
			Name: "gsi1",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

var numericSortKeyTable = table.TableDefinition{
	Name: "numeric-sk-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindN},
	},
}

var noSortKeyTable = table.TableDefinition{
	Name: "no-sk-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
	},
}

func newTestStore(t *testing.T, defs ...table.TableDefinition) *Store {
	store, err := New(StoreOptions{InMemory: true}, defs...)
	require.NoError(t, err)
	t.Cleanup(func() {
		store.Close()
	})
	return store
}
