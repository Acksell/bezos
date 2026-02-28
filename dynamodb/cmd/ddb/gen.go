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

	return ddbgen.RunGenerate(ddbgen.GenerateOptions{
		Dir:      *dir,
		Output:   *output,
		NoSchema: *noSchema,
	})
}
