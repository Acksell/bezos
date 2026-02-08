// Package codegen implements the code generation logic for ddbgen.
package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"reflect"
	"strings"
	"text/template"

	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/keys"
)

// IndexBinding holds an index with its code generation metadata.
type IndexBinding struct {
	// Name is the logical name for this index (e.g., "User", "Order")
	Name string
	// EntityType is an instance of the entity struct stored in this index.
	EntityType any
	// Index is the PrimaryIndex definition.
	Index index.PrimaryIndex
	// VarName is the variable name of the original index in user code.
	// If empty, defaults to "<Name>Index".
	VarName string
}

// Config holds the code generation configuration.
type Config struct {
	// Package is the Go package name for generated code.
	Package string
	// Indexes are the index bindings to generate code for.
	Indexes []IndexBinding
}

// Generator generates Go code from index definitions.
type Generator struct {
	Config Config
}

// New creates a new Generator for the given config.
func New(cfg Config) *Generator {
	return &Generator{Config: cfg}
}

// Generate produces the generated Go code.
func (g *Generator) Generate() ([]byte, error) {
	var buf bytes.Buffer

	// Build template data by introspecting the indexes
	indexes := make([]indexData, 0, len(g.Config.Indexes))
	for _, binding := range g.Config.Indexes {
		// Build tag map from entity type if provided
		var tagMap map[string]FieldMapping
		var entityTypeName string
		if binding.EntityType != nil {
			var err error
			tagMap, err = BuildTagToFieldMap(binding.EntityType)
			if err != nil {
				return nil, fmt.Errorf("building tag map for %q: %w", binding.Name, err)
			}
			entityTypeName = getTypeName(binding.EntityType)
		}

		data, err := introspectIndex(binding.Name, binding.Index, tagMap)
		if err != nil {
			return nil, fmt.Errorf("introspecting index %q: %w", binding.Name, err)
		}
		data.EntityType = entityTypeName

		// Set the index variable name from binding or use default
		if binding.VarName != "" {
			data.IndexVarName = binding.VarName
		} else {
			data.IndexVarName = binding.Name + "Index"
		}
		indexes = append(indexes, data)
	}

	// Execute the main template
	tmpl, err := template.New("main").Funcs(templateFuncs).Parse(mainTemplate)
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	data := templateData{
		Package: g.Config.Package,
		Imports: buildImports(),
		Indexes: indexes,
	}

	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("executing template: %w", err)
	}

	// Format the generated code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		// Return unformatted code with error for debugging
		return buf.Bytes(), fmt.Errorf("formatting generated code: %w", err)
	}

	return formatted, nil
}

// templateData is the data passed to the template.
type templateData struct {
	Package string
	Imports []string
	Indexes []indexData
}

// indexData holds introspected data about an index for code generation.
type indexData struct {
	Name         string
	IndexVarName string // The variable name of the original index (from user code)
	EntityType   string // The Go type name of the entity (e.g., "User")
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
	GSIs         []gsiData
}

// HasEntity returns true if this index has an associated entity type.
func (d indexData) HasEntity() bool {
	return d.EntityType != ""
}

// keyData holds introspected data about a key extractor.
type keyData struct {
	Params           []paramData // Function parameters derived from the key
	FormatExpr       string      // Go expression to format the key value (using params)
	EntityFormatExpr string      // Go expression to format using entity fields (e.g., fmt.Sprintf("USER#%s", e.UserID))
}

// paramData represents a function parameter.
type paramData struct {
	Name string
	Type string
}

// gsiData holds introspected data about a GSI.
type gsiData struct {
	Name         string
	Index        int // Index in the Secondary slice
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
}

func buildImports() []string {
	return []string{
		`"fmt"`,
		`"github.com/acksell/bezos/dynamodb/ddbsdk"`,
		`"github.com/acksell/bezos/dynamodb/index"`,
		`"github.com/acksell/bezos/dynamodb/table"`,
	}
}

// getTypeName returns the simple type name from an instance.
func getTypeName(v any) string {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// introspectIndex extracts code generation data from a PrimaryIndex.
func introspectIndex(name string, idx index.PrimaryIndex, tagMap map[string]FieldMapping) (indexData, error) {
	if name == "" {
		return indexData{}, fmt.Errorf("index must have a Name for code generation")
	}

	pkData, err := introspectExtractor(idx.PartitionKey, tagMap)
	if err != nil {
		return indexData{}, fmt.Errorf("partition key: %w", err)
	}

	data := indexData{
		Name:         name,
		PartitionKey: pkData,
	}

	if idx.Table.KeyDefinitions.SortKey.Name != "" && idx.SortKey != nil {
		skData, err := introspectExtractor(idx.SortKey, tagMap)
		if err != nil {
			return indexData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	// Introspect GSIs
	for i, gsi := range idx.Secondary {
		gsiData, err := introspectGSI(i, gsi, tagMap)
		if err != nil {
			return indexData{}, fmt.Errorf("GSI %q: %w", gsi.Name, err)
		}
		data.GSIs = append(data.GSIs, gsiData)
	}

	return data, nil
}

// introspectGSI extracts code generation data from a SecondaryIndex.
func introspectGSI(idx int, gsi index.SecondaryIndex, tagMap map[string]FieldMapping) (gsiData, error) {
	pkData, err := introspectExtractor(gsi.PartitionKey.Extractor, tagMap)
	if err != nil {
		return gsiData{}, fmt.Errorf("partition key: %w", err)
	}

	data := gsiData{
		Name:         gsi.Name,
		Index:        idx,
		PartitionKey: pkData,
	}

	if gsi.SortKey != nil {
		skData, err := introspectExtractor(gsi.SortKey.Extractor, tagMap)
		if err != nil {
			return gsiData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	return data, nil
}

// introspectExtractor extracts parameter and format expression data from an Extractor.
func introspectExtractor(ext keys.Extractor, tagMap map[string]FieldMapping) (keyData, error) {
	var params []paramData
	var formatParts []string
	var entityFormatParts []string

	switch e := ext.(type) {
	case keys.ConstVal:
		// Constant value - no parameters, just the value
		formatParts = append(formatParts, fmt.Sprintf("%q", e.Value))
		entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", e.Value))

	case keys.FieldRef:
		// Single field - one parameter
		tagPath := strings.Join(e.Path, ".")
		paramName := e.Path[len(e.Path)-1] // Use the last path element as param name
		params = append(params, paramData{Name: paramName, Type: "string"})
		formatParts = append(formatParts, paramName)

		// Find the Go field from tag map (required if entity type is configured)
		if tagMap != nil {
			if mapping, ok := tagMap[tagPath]; ok {
				entityFormatParts = append(entityFormatParts, mapping.GoExpr)
			} else {
				return keyData{}, fmt.Errorf("no struct field found with tag %q in entity type", tagPath)
			}
		}

	case keys.FormatExpr:
		// Composite format - extract from parts
		for _, part := range e.Parts {
			switch p := part.(type) {
			case keys.ConstVal:
				// Constant part - add to format string
				formatParts = append(formatParts, fmt.Sprintf("%q", p.Value))
				entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", p.Value))
			case keys.FieldRef:
				// Field part - add parameter
				tagPath := strings.Join(p.Path, ".")
				paramName := p.Path[len(p.Path)-1]
				params = append(params, paramData{Name: paramName, Type: "string"})
				formatParts = append(formatParts, paramName)

				// Find the Go field from tag map (required if entity type is configured)
				if tagMap != nil {
					if mapping, ok := tagMap[tagPath]; ok {
						entityFormatParts = append(entityFormatParts, mapping.GoExpr)
					} else {
						return keyData{}, fmt.Errorf("no struct field found with tag %q in entity type", tagPath)
					}
				}
			default:
				return keyData{}, fmt.Errorf("unsupported extractor type in format: %T", p)
			}
		}

	default:
		return keyData{}, fmt.Errorf("unsupported extractor type: %T", ext)
	}

	// Build the format expressions
	formatExpr := buildFormatExpr(params, formatParts)
	entityFormatExpr := buildEntityFormatExpr(entityFormatParts)

	return keyData{
		Params:           params,
		FormatExpr:       formatExpr,
		EntityFormatExpr: entityFormatExpr,
	}, nil
}

// buildFormatExpr builds a Go expression to format a key value.
func buildFormatExpr(params []paramData, parts []string) string {
	if len(params) == 0 {
		// No parameters - just return the constant
		if len(parts) == 1 {
			return parts[0]
		}
		// Multiple constants - concatenate them
		return strings.Join(parts, " + ")
	}

	// Build fmt.Sprintf call
	var fmtParts []string
	var args []string

	for _, part := range parts {
		// Check if this part is a parameter name
		isParam := false
		for _, p := range params {
			if part == p.Name {
				isParam = true
				args = append(args, part)
				fmtParts = append(fmtParts, "%s")
				break
			}
		}
		if !isParam {
			// It's a quoted constant - extract the value
			fmtParts = append(fmtParts, strings.Trim(part, `"`))
		}
	}

	formatStr := strings.Join(fmtParts, "")
	if len(args) == 0 {
		return fmt.Sprintf("%q", formatStr)
	}
	return fmt.Sprintf("fmt.Sprintf(%q, %s)", formatStr, strings.Join(args, ", "))
}

// buildEntityFormatExpr builds a Go expression to format a key value from entity fields.
// parts contains either quoted constants (e.g., `"USER#"`) or Go expressions (e.g., `e.UserID`).
func buildEntityFormatExpr(parts []string) string {
	if len(parts) == 0 {
		return `""`
	}
	if len(parts) == 1 {
		return parts[0]
	}

	// Build fmt.Sprintf call
	var fmtParts []string
	var args []string

	for _, part := range parts {
		if strings.HasPrefix(part, `"`) {
			// Quoted constant - extract the value for format string
			fmtParts = append(fmtParts, strings.Trim(part, `"`))
		} else {
			// Go expression (e.g., e.UserID) - use %s placeholder
			fmtParts = append(fmtParts, "%s")
			args = append(args, part)
		}
	}

	formatStr := strings.Join(fmtParts, "")
	if len(args) == 0 {
		return fmt.Sprintf("%q", formatStr)
	}
	return fmt.Sprintf("fmt.Sprintf(%q, %s)", formatStr, strings.Join(args, ", "))
}

var templateFuncs = template.FuncMap{
	"allParams": func(idx indexData) string {
		var parts []string
		for _, p := range idx.PartitionKey.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		if idx.SortKey != nil {
			for _, p := range idx.SortKey.Params {
				parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
			}
		}
		return strings.Join(parts, ", ")
	},
	"allArgs": func(idx indexData) string {
		var args []string
		for _, p := range idx.PartitionKey.Params {
			args = append(args, p.Name)
		}
		if idx.SortKey != nil {
			for _, p := range idx.SortKey.Params {
				args = append(args, p.Name)
			}
		}
		return strings.Join(args, ", ")
	},
	"gsiAllParams": func(gsi gsiData) string {
		var parts []string
		for _, p := range gsi.PartitionKey.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		if gsi.SortKey != nil {
			for _, p := range gsi.SortKey.Params {
				parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
			}
		}
		return strings.Join(parts, ", ")
	},
	"gsiAllArgs": func(gsi gsiData) string {
		var args []string
		for _, p := range gsi.PartitionKey.Params {
			args = append(args, p.Name)
		}
		if gsi.SortKey != nil {
			for _, p := range gsi.SortKey.Params {
				args = append(args, p.Name)
			}
		}
		return strings.Join(args, ", ")
	},
}

const mainTemplate = `// Code generated by ddbgen. DO NOT EDIT.

package {{.Package}}

import (
{{- range .Imports}}
	{{.}}
{{- end}}
)

{{range $idx := .Indexes}}
// =============================================================================
// {{$idx.Name}} Index Wrapper
// =============================================================================

// {{$idx.Name}}IndexType wraps the PrimaryIndex with strongly-typed methods.
type {{$idx.Name}}IndexType struct {
	index.PrimaryIndex
}

// {{$idx.Name}}Index is the typed wrapper for {{$idx.Name}} operations.
var {{$idx.Name}}Index = {{$idx.Name}}IndexType{PrimaryIndex: *{{$idx.IndexVarName}}}

// PrimaryKey creates a strongly-typed primary key from explicit parameters.
func (idx {{$idx.Name}}IndexType) PrimaryKey({{allParams $idx}}) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: idx.Table.KeyDefinitions,
		Values: table.PrimaryKeyValues{
			PartitionKey: {{$idx.PartitionKey.FormatExpr}},
			{{- if $idx.HasSortKey}}
			SortKey:      {{$idx.SortKey.FormatExpr}},
			{{- end}}
		},
	}
}
{{if $idx.HasEntity}}
// PrimaryKeyFrom extracts the primary key from a {{$idx.EntityType}} entity.
func (idx {{$idx.Name}}IndexType) PrimaryKeyFrom(e *{{$idx.EntityType}}) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: idx.Table.KeyDefinitions,
		Values: table.PrimaryKeyValues{
			PartitionKey: {{$idx.PartitionKey.EntityFormatExpr}},
			{{- if $idx.HasSortKey}}
			SortKey:      {{$idx.SortKey.EntityFormatExpr}},
			{{- end}}
		},
	}
}

// NewUnsafePut creates a Put operation without optimistic locking.
func (idx {{$idx.Name}}IndexType) NewUnsafePut(e *{{$idx.EntityType}}) *ddbsdk.Put {
	return ddbsdk.NewUnsafePut(idx.PrimaryIndex, idx.PrimaryKeyFrom(e), e)
}
{{end}}
// NewDelete creates a Delete operation.
func (idx {{$idx.Name}}IndexType) NewDelete({{allParams $idx}}) *ddbsdk.Delete {
	return ddbsdk.NewDelete(idx.PrimaryIndex, idx.PrimaryKey({{allArgs $idx}}))
}

// NewUnsafeUpdate creates an Update operation without optimistic locking.
func (idx {{$idx.Name}}IndexType) NewUnsafeUpdate({{allParams $idx}}) *ddbsdk.UnsafeUpdate {
	return ddbsdk.NewUnsafeUpdate(idx.PrimaryIndex, idx.PrimaryKey({{allArgs $idx}}))
}

{{range $gsi := $idx.GSIs}}
// {{$gsi.Name}}Key creates a key for querying the {{$gsi.Name}} GSI.
func (idx {{$idx.Name}}IndexType) {{$gsi.Name}}Key({{gsiAllParams $gsi}}) table.PrimaryKey {
	return table.PrimaryKey{
		Definition: idx.Secondary[{{$gsi.Index}}].KeyDefinition(),
		Values: table.PrimaryKeyValues{
			PartitionKey: {{$gsi.PartitionKey.FormatExpr}},
			{{- if $gsi.HasSortKey}}
			SortKey:      {{$gsi.SortKey.FormatExpr}},
			{{- end}}
		},
	}
}
{{end}}
{{end}}
`
