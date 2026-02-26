// ddbgen is a code generator for type-safe DynamoDB key constructors.
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
// The generator scans the current package for PrimaryIndex definitions
// and generates type-safe key constructors.
//
// # Schema Generation
//
// Use --schema to generate YAML schema files alongside the Go code:
//
//	ddbgen --schema
//
// This creates schema_{tablename}.yaml files that describe table structure
// and entity mappings. These files can be used by ddbui for visualization.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbgen"
)

func main() {
	var (
		dir       = flag.String("dir", ".", "directory to scan for index definitions")
		output    = flag.String("output", "index_gen.go", "output file path")
		genSchema = flag.Bool("schema", false, "generate schema YAML files for ddbui")
	)
	flag.Parse()

	if err := run(*dir, *output, *genSchema); err != nil {
		fmt.Fprintf(os.Stderr, "ddbgen: %v\n", err)
		os.Exit(1)
	}
}

func run(dir, output string, genSchema bool) error {
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

	// Generate schema files if requested
	if genSchema {
		schemas, err := ddbgen.GenerateSchemas(result)
		if err != nil {
			return fmt.Errorf("generating schemas: %w", err)
		}

		if err := ddbgen.WriteSchemas(schemas, dir); err != nil {
			return fmt.Errorf("writing schema files: %w", err)
		}

		fmt.Printf("ddbgen: generated %d schema file(s)\n", len(schemas))
	}

	return nil
}
