package ddbgen

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbsdk"
	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/indices"
	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

// GenerateOptions configures code generation behavior.
type GenerateOptions struct {
	// Dir is the output directory for generated code.
	Dir string
	// Output is the output file name for generated Go code.
	Output string
	// PackageName is the Go package name for generated code.
	PackageName string
	// NoSchema disables schema/ subdirectory generation.
	NoSchema bool
}

// Generate produces generated code from all registered PrimaryIndex definitions.
// Call this from a gen/main.go after importing the package that registers indexes.
//
// Example:
//
//	package main
//
//	import (
//	    _ "myapp/entities"  // side-effect import populates registry
//	    "github.com/acksell/bezos/dynamodb/ddbgen"
//	)
//
//	func main() {
//	    ddbgen.Generate(ddbgen.GenerateOptions{
//	        Dir:         ".",
//	        PackageName: "entities",
//	    })
//	}
func Generate(opts GenerateOptions) error {
	if opts.Dir == "" {
		opts.Dir = "."
	}
	if opts.Output == "" {
		opts.Output = "index_gen.go"
	}
	if opts.PackageName == "" {
		opts.PackageName = "main"
	}

	entries := indices.All()
	if len(entries) == 0 {
		fmt.Fprintf(os.Stderr, "ddbgen: no registered indexes found\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Register indexes in your package using indices.Add:\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  var _ = indices.Add(index.PrimaryIndex[MyEntity]{...})\n")
		return nil
	}

	// Convert registry entries to IndexInfo using reflection.
	indexInfos := make([]IndexInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entryToIndexInfo(entry)
		if err != nil {
			return fmt.Errorf("processing %s: %w", entry.EntityType.Name(), err)
		}
		indexInfos = append(indexInfos, info)
	}

	// Generate code.
	code, err := GenerateCode(opts.PackageName, indexInfos)
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	// Write output.
	outputPath := filepath.Join(opts.Dir, opts.Output)
	absOutput, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("resolving output path: %w", err)
	}

	if err := os.WriteFile(absOutput, code, 0644); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	fmt.Printf("ddbgen: generated %s (%d indexes)\n", absOutput, len(indexInfos))

	// Generate schema files.
	if !opts.NoSchema {
		absDir, err := filepath.Abs(opts.Dir)
		if err != nil {
			return fmt.Errorf("resolving directory: %w", err)
		}
		schemaDir := filepath.Join(absDir, "schema")
		if err := os.MkdirAll(schemaDir, 0755); err != nil {
			return fmt.Errorf("creating schema directory: %w", err)
		}
		if err := generateSchemaFiles(schemaDir, indexInfos); err != nil {
			return fmt.Errorf("generating schema: %w", err)
		}
	}

	return nil
}

// entryToIndexInfo converts an indices.Entry to IndexInfo using reflection.
// All the heavy reflection work happens here, keeping the indices package minimal.
func entryToIndexInfo(entry indices.Entry) (IndexInfo, error) {
	entityType := entry.EntityType
	entityName := entityType.Name()

	// Extract PrimaryIndex fields via reflection.
	rv := reflect.ValueOf(entry.Index)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	tableField := rv.FieldByName("Table")
	pkField := rv.FieldByName("PartitionKey")
	skField := rv.FieldByName("SortKey")
	secondaryField := rv.FieldByName("Secondary")

	tbl, ok := tableField.Interface().(table.TableDefinition)
	if !ok {
		return IndexInfo{}, fmt.Errorf("Table field is not table.TableDefinition")
	}

	pk, ok := pkField.Interface().(val.ValDef)
	if !ok {
		return IndexInfo{}, fmt.Errorf("PartitionKey field is not val.ValDef")
	}

	var sk *val.ValDef
	if !skField.IsNil() {
		skVal, ok := skField.Interface().(*val.ValDef)
		if !ok {
			return IndexInfo{}, fmt.Errorf("SortKey field is not *val.ValDef")
		}
		sk = skVal
	}

	// Build GSI info.
	var gsis []GSIInfo
	if secondaryField.IsValid() && secondaryField.Kind() == reflect.Slice && secondaryField.Len() > 0 {
		for i := 0; i < secondaryField.Len(); i++ {
			sec := secondaryField.Index(i).Interface().(index.SecondaryIndex)
			gsis = append(gsis, GSIInfo{
				Name:      sec.GSI.Name,
				Index:     i,
				PKDef:     sec.GSI.KeyDefinitions.PartitionKey.Name,
				PKPattern: sec.Partition,
				SKDef:     sec.GSI.KeyDefinitions.SortKey.Name,
				SKPattern: sec.Sort,
			})
		}
	}

	// Extract entity struct fields with dynamodbav tags.
	var fields []FieldInfo
	if entityType.Kind() == reflect.Struct {
		for i := 0; i < entityType.NumField(); i++ {
			f := entityType.Field(i)
			tag := f.Tag.Get("dynamodbav")
			if tag == "" || tag == "-" {
				continue
			}
			if commaIdx := strings.IndexByte(tag, ','); commaIdx != -1 {
				tag = tag[:commaIdx]
			}
			fields = append(fields, FieldInfo{
				Name: f.Name,
				Tag:  tag,
				Type: reflectTypeString(f.Type),
			})
		}
	}

	// Check if entity implements VersionedDynamoEntity.
	isVersioned := false
	zeroVal := reflect.New(entityType).Interface()
	if _, ok := zeroVal.(ddbsdk.VersionedDynamoEntity); ok {
		isVersioned = true
	}

	return IndexInfo{
		VarName:      entityName + "Index",
		EntityType:   entityName,
		TableName:    tbl.Name,
		PKDefName:    tbl.KeyDefinitions.PartitionKey.Name,
		SKDefName:    tbl.KeyDefinitions.SortKey.Name,
		PartitionKey: pk,
		SortKey:      sk,
		GSIs:         gsis,
		IsVersioned:  isVersioned,
		Fields:       fields,
	}, nil
}

// reflectTypeString returns a string representation of a reflect.Type.
func reflectTypeString(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return "*" + reflectTypeString(t.Elem())
	case reflect.Slice:
		return "[]" + reflectTypeString(t.Elem())
	case reflect.Map:
		return "map[" + reflectTypeString(t.Key()) + "]" + reflectTypeString(t.Elem())
	default:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return "time.Time"
		}
		if t.PkgPath() != "" {
			return t.Name()
		}
		return t.String()
	}
}
