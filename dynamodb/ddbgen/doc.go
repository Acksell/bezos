// Package ddbgen provides code generation for type-safe DynamoDB operations.
//
// # Installation
//
//	go install github.com/acksell/bezos/dynamodb/ddbgen/cmd/ddbgen@latest
//
// # Usage
//
// Add a go:generate directive to your package that contains the index definitions.
//
//	//go:generate ddbgen
//
//	var userIndex = index.PrimaryIndex[User]{
//	    Table:        UserTable,
//	    PartitionKey: "USER#{id}",
//	    SortKey:      "PROFILE",
//	}
//
// The generator will scan the package, discover index definitions,
// and generate type-safe key constructors.
package ddbgen
