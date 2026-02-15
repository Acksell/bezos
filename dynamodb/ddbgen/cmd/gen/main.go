// ddbgen is a code generator for type-safe DynamoDB key constructors.
//
// Usage:
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
// The generator scans the current package for PrimaryIndex definitions
// and generates type-safe key constructors.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbgen"
)

func main() {
	var (
		dir    = flag.String("dir", ".", "directory to scan for index definitions")
		output = flag.String("output", "index_gen.go", "output file path")
	)
	flag.Parse()

	if err := run(*dir, *output); err != nil {
		fmt.Fprintf(os.Stderr, "ddbgen: %v\n", err)
		os.Exit(1)
	}
}

func run(dir, output string) error {
	result, err := ddbgen.Discover(dir)
	if err != nil {
		return fmt.Errorf("discovering indexes: %w", err)
	}

	if len(result.Indexes) == 0 {
		fmt.Fprintf(os.Stderr, "ddbgen: no index.PrimaryIndex definitions found in %s\n", dir)
		return nil
	}

	code, err := ddbgen.Generate(result)
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	if err := os.WriteFile(output, code, 0644); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	fmt.Printf("ddbgen: generated %s (%d indexes)\n", output, len(result.Indexes))
	return nil
}
