// Package example demonstrates how to use ddbgen with index definitions.
//
// Define indexes using PrimaryIndex with type parameters and string patterns.
// Run the generator to produce type-safe key constructors:
//
//go:generate go run github.com/acksell/bezos/dynamodb/ddbgen/cmd/ddbgen
package example

import (
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
	GSIs: []table.TableDefinition{
		{
			Name:  "ByEmail",
			IsGSI: true,
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

var userIndex = index.PrimaryIndex[User]{
	Table:        UserTable,
	PartitionKey: keys.Fmt("USER#{id}"),
	SortKey:      keys.Fmt("PROFILE").Ptr(),
	// todo do we even need secondary indexes?
	// Can't we just use GSI table definition directly?
	// What is a primaryindex on a GSI?
	// Is this just a way for extracting GSI keys? Do we need that?
	Secondary: []index.SecondaryIndex{
		{
			Name: "ByEmail",
			Partition: index.KeyValDef{
				KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				ValDef: keys.Fmt("EMAIL#{email}"),
			},
			Sort: &index.KeyValDef{
				KeyDef: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
				ValDef: keys.Fmt("USER#{id}"),
			},
		},
	},
}

var OrderTable = table.TableDefinition{
	Name: "orders",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

var orderIndex = index.PrimaryIndex[Order]{
	Table:        OrderTable,
	PartitionKey: keys.Fmt("TENANT#{tenantID}"),
	SortKey:      keys.Fmt("ORDER#{orderID}").Ptr(),
}
