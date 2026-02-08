// Package ddbgen provides code generation for type-safe DynamoDB operations.
//
// Use BindIndex to wrap index definitions for code generation:
//
//	var UserIndex = ddbgen.BindIndex(User{}, index.PrimaryIndex{
//	    Table:        UserTable,
//	    PartitionKey: keys.Fmt("USER#%s", keys.Field("id")),
//	    SortKey:      keys.Const("PROFILE"),
//	})
//
// Then create a generate command that imports your package and calls Generate:
//
//	//go:generate go run ./cmd/generate
//
//	// cmd/generate/main.go
//	import (
//	    _ "myapp/db"  // Side-effect import triggers registration
//	    "github.com/acksell/bezos/dynamodb/ddbgen"
//	)
//
//	func main() {
//	    ddbgen.Generate(ddbgen.Config{
//	        Package: "db",
//	        Output:  "keys_gen.go",
//	    })
//	}
package ddbgen

import (
	"reflect"

	"github.com/acksell/bezos/dynamodb/index"
)

// IndexBinding holds the code generation metadata for an index.
type IndexBinding struct {
	// Name is the logical name for this index, derived from the entity type name.
	Name string
	// EntityType is an instance of the entity struct stored in this index.
	EntityType any
	// Index is a pointer to the PrimaryIndex being bound.
	Index *index.PrimaryIndex
}

var registry []IndexBinding

// BindIndex wraps a PrimaryIndex with an entity type for code generation.
// The index name is inferred from the entity type name via reflection.
// Returns a pointer to the index for use as a package-level variable.
//
// Example:
//
//	var UserIndex = ddbgen.BindIndex(User{}, index.PrimaryIndex{
//	    Table:        UserTable,
//	    PartitionKey: keys.Fmt("USER#%s", keys.Field("id")),
//	})
func BindIndex(entityType any, idx index.PrimaryIndex) *index.PrimaryIndex {
	name := reflect.TypeOf(entityType).Name()
	ptr := &idx
	registry = append(registry, IndexBinding{
		Name:       name,
		EntityType: entityType,
		Index:      ptr,
	})
	return ptr
}

// Registered returns all indexes that have been registered via BindIndex().
// This is used by the code generator to discover which indexes to process.
func Registered() []IndexBinding {
	return registry
}

// ClearRegistry removes all registered indexes. Useful for testing.
func ClearRegistry() {
	registry = nil
}
