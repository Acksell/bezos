// Package example demonstrates how to use ddbgen with index definitions.
//
// Define indexes using PrimaryIndex with type parameters and string patterns.
// Run the generator to produce type-safe key constructors:
//
//go:generate go run github.com/acksell/bezos/dynamodb/ddbgen/cmd/ddbgen
package example

import (
	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

var UserTable = table.TableDefinition{
	Name: "users",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	GSIs: []table.GSIDefinition{
		{
			Name: "ByEmail",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

var userIndex = index.PrimaryIndex[User]{
	Table:        UserTable,
	PartitionKey: val.Fmt("USER#{id}"),
	SortKey:      val.Fmt("PROFILE").Ptr(),
	Secondary: []index.SecondaryIndex{
		{
			Name: "ByEmail",
			Partition: index.KeyValDef{
				KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				ValDef: val.Fmt("EMAIL#{email}"),
			},
			Sort: &index.KeyValDef{
				KeyDef: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
				ValDef: val.Fmt("USER#{id}"),
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
	PartitionKey: val.Fmt("TENANT#{tenantID}"),
	SortKey:      val.Fmt("ORDER#{orderID}").Ptr(),
}
