// Package ddbgen provides code generation for type-safe DynamoDB operations.
//
// # Installation
//
//	go install github.com/acksell/bezos/dynamodb/cmd/ddb@latest
//
// # Usage
//
// Register your indexes using [indices.Add] and add a go:generate directive:
//
//	//go:generate ddb gen
//
//	var _ = indices.Add(index.PrimaryIndex[User]{
//	    Table:        UserTable,
//	    PartitionKey: val.Fmt("USER#{id}"),
//	    SortKey:      val.Fmt("PROFILE").Ptr(),
//	})
//
// Running go generate will:
//  1. Bootstrap gen/main.go if it doesn't exist
//  2. Run gen/main.go which calls [Generate] to produce index_gen.go and schema/ files
//
// The gen/main.go file is persistent and can be committed to version control.
// It imports your package (populating the indices registry via side-effect)
// and then calls [Generate] with the appropriate options.
//
// You can also run ddb gen from the CLI to regenerate all packages at once:
//
//	ddb gen
package ddbgen
