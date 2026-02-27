package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbgen"
)

func runGen() error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)

	var (
		dir       = fs.String("dir", ".", "directory to scan for index definitions")
		output    = fs.String("output", "index_gen.go", "output file path for generated Go code")
		noYAML    = fs.Bool("no-yaml", false, "disable schema YAML file generation")
		schemaDir = fs.String("schema-dir", "", "directory for schema YAML files (default: same as source)")
	)

	fs.Usage = func() {
		fmt.Println(`ddb gen - Generate type-safe key constructors and schema files

Usage:
  ddb gen [flags]

Flags:`)
		fs.PrintDefaults()
		fmt.Println(`
Examples:
  ddb gen                           # Generate code and schema files
  ddb gen --no-yaml                 # Generate code only
  ddb gen --dir ./entities          # Scan specific directory
  ddb gen --schema-dir ./schemas    # Output schemas to specific directory

Typical usage with go:generate:
  //go:generate ddb gen
  //go:generate ddb gen --schema-dir ../../schemas

The generator scans for index.PrimaryIndex definitions and produces:
  - index_gen.go: Type-safe key constructors
  - schema_*.yaml: Table/entity schema files (unless --no-yaml)`)
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	// Determine schema output directory
	schemaOutputDir := *dir
	if *schemaDir != "" {
		schemaOutputDir = *schemaDir
	}

	// Discover indexes
	result, err := ddbgen.Discover(*dir)
	if err != nil {
		return fmt.Errorf("discovering indexes: %w", err)
	}

	if len(result.Indexes) == 0 {
		fmt.Fprintf(os.Stderr, "ddb gen: no index.PrimaryIndex definitions found in %s\n", *dir)
		return nil
	}

	// Generate Go code
	code, err := ddbgen.Generate(result)
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	outputPath := *output
	if *dir != "." {
		outputPath = *dir + "/" + *output
	}

	if err := os.WriteFile(outputPath, code, 0644); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	fmt.Printf("ddb gen: generated %s (%d indexes)\n", outputPath, len(result.Indexes))

	// Generate schema file unless disabled
	if !*noYAML {
		schemas, err := ddbgen.GenerateSchemas(result)
		if err != nil {
			return fmt.Errorf("generating schemas: %w", err)
		}

		// Ensure schema directory exists
		if schemaOutputDir != "" && schemaOutputDir != "." {
			if err := os.MkdirAll(schemaOutputDir, 0755); err != nil {
				return fmt.Errorf("creating schema directory: %w", err)
			}
		}

		if err := ddbgen.WriteSchemas(schemas, schemaOutputDir); err != nil {
			return fmt.Errorf("writing schema file: %w", err)
		}

		schemaPath := schemaOutputDir
		if schemaPath == "" || schemaPath == "." {
			schemaPath = "schema_dynamodb.yaml"
		} else {
			schemaPath = schemaPath + "/schema_dynamodb.yaml"
		}
		fmt.Printf("ddb gen: generated %s (%d tables)\n", schemaPath, len(schemas))
	}

	return nil
}
