// Package example demonstrates how to use ddbgen with index definitions.
//
// This file defines the indexes using ddbgen.BindIndex() for code generation.
// Run the generator in cmd/generate to produce keys_gen.go.
//
//go:generate go run ./cmd/generate
package example

import (
	"github.com/acksell/bezos/dynamodb/ddbgen"
	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/keys"
	"github.com/acksell/bezos/dynamodb/table"
)

var UserTable = table.TableDefinition{
	Name: "users",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

var userIndex = ddbgen.BindIndex(User{}, index.PrimaryIndex{
	Table:        UserTable,
	PartitionKey: keys.Fmt("USER#%s", keys.Field("id")),
	SortKey:      keys.Const("PROFILE"),
	Secondary: []index.SecondaryIndex{
		{
			Name: "ByEmail",
			PartitionKey: keys.Key{
				Def:       table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				Extractor: keys.Fmt("EMAIL#%s", keys.Field("email")),
			},
			SortKey: &keys.Key{
				Def:       table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
				Extractor: keys.Fmt("USER#%s", keys.Field("id")),
			},
		},
	},
})

var OrderTable = table.TableDefinition{
	Name: "orders",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

var orderIndex = ddbgen.BindIndex(Order{}, index.PrimaryIndex{
	Table:        OrderTable,
	PartitionKey: keys.Fmt("TENANT#%s", keys.Field("tenantID")),
	SortKey:      keys.Fmt("ORDER#%s", keys.Field("orderID")),
})
