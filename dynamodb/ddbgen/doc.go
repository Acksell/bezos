// Package ddbgen provides code generation for type-safe DynamoDB operations.
//
// Use go:generate to run the code generator:
//
//	//go:generate go run github.com/acksell/bezos/dynamodb/ddbgen/cmd/ddbgen
//
// Define indexes using PrimaryIndex with an entity type parameter:
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
