// Package ddbgen provides code generation for type-safe DynamoDB operations.
//
// # Installation
//
//	go install github.com/acksell/bezos/dynamodb/cmd/ddb@latest
//
// # Usage
//
// Add a go:generate directive to your package that contains the index definitions.
//
//	//go:generate ddb gen
//
//	var userIndex = index.PrimaryIndex[User]{
//	    Table:        UserTable,
//	    PartitionKey: val.Fmt("USER#{id}"),
//	    SortKey:      val.Fmt("PROFILE").Ptr(),
//	}
//
// The generator will scan the package, discover index definitions,
// and generate type-safe key constructors plus schema YAML files.
//
// To generate code only (no schema files):
//
//	ddb gen --no-yaml
package ddbgen
