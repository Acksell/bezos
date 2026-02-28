package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/acksell/bezos/dynamodb/ddbgen"
)

func runGen() error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)

	var (
		dir      = fs.String("dir", ".", "directory to scan for index definitions")
		output   = fs.String("output", "index_gen.go", "output file path for generated Go code")
		noSchema = fs.Bool("no-schema", false, "disable schema/ subdirectory generation")
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
  ddb gen --no-schema               # Generate code only (no schema/ subdirectory)
  ddb gen --dir ./entities          # Scan specific directory

Typical usage with go:generate:
  //go:generate ddb gen

The generator scans for index.PrimaryIndex definitions and produces:
  - index_gen.go: Type-safe key constructors
  - schema/schema_dynamodb.yaml: Table/entity schema in YAML format
  - schema/schema_gen.go: Go loader that embeds the YAML and exports Schema variable

The schema/ package can be imported to pass to ddbui.NewServer:
  import "myapp/entities/schema"
  server, _ := ddbui.NewServer(store, 3070, schema.Schema)`)
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
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

	// Generate schema/ subdirectory with YAML and Go loader
	if !*noSchema {
		schemas, err := ddbgen.GenerateSchemas(result)
		if err != nil {
			return fmt.Errorf("generating schemas: %w", err)
		}

		schemaDir := filepath.Join(*dir, "schema")
		if err := os.MkdirAll(schemaDir, 0755); err != nil {
			return fmt.Errorf("creating schema directory: %w", err)
		}

		// Write YAML schema file
		if err := ddbgen.WriteSchemas(schemas, schemaDir); err != nil {
			return fmt.Errorf("writing schema file: %w", err)
		}
		fmt.Printf("ddb gen: generated %s (%d tables)\n", filepath.Join(schemaDir, "schema_dynamodb.yaml"), len(schemas.Tables))

		// Write Go loader file
		schemaGoCode, err := ddbgen.GenerateSchemaGo()
		if err != nil {
			return fmt.Errorf("generating schema go code: %w", err)
		}

		schemaGoPath := filepath.Join(schemaDir, "schema_gen.go")
		if err := os.WriteFile(schemaGoPath, schemaGoCode, 0644); err != nil {
			return fmt.Errorf("writing schema go file: %w", err)
		}

		fmt.Printf("ddb gen: generated %s\n", schemaGoPath)
	}

	return nil
}
