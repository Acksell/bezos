package index

import (
	"github.com/acksell/bezos/dynamodb/index/keys"
	"github.com/acksell/bezos/dynamodb/table"
)

// KeyValDef combines a key definition with a value definition.
//
// Examples:
//
//	// Using a format pattern:
//	index.KeyValDef{
//	    KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
//	    ValDef: keys.Fmt("EMAIL#{email}"),
//	}
//
//	// Copying from an existing field:
//	index.KeyValDef{
//	    KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
//	    ValDef: keys.FromField("email"),
//	}
//
//	// Using a constant value:
//	index.KeyValDef{
//	    KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
//	    ValDef: keys.String("PROFILE"),
//	}
type KeyValDef struct {
	KeyDef table.KeyDef
	ValDef keys.ValDef
}
