package ddbgen

import (
	"bytes"
	"fmt"
	"go/format"
	"regexp"
	"strings"
	"text/template"
)

// Generate produces type-safe key constructor code from discovered indexes.
func Generate(result *DiscoverResult) ([]byte, error) {
	if len(result.Indexes) == 0 {
		return nil, fmt.Errorf("no indexes to generate")
	}

	// Convert discovery result to template data
	indexes := make([]indexData, 0, len(result.Indexes))
	for _, idx := range result.Indexes {
		data, err := buildIndexData(idx, result.EntityFields[idx.EntityType])
		if err != nil {
			return nil, fmt.Errorf("building data for %s: %w", idx.VarName, err)
		}
		indexes = append(indexes, data)
	}

	tmplData := templateData{
		Package: result.PackageName,
		Imports: []string{
			`"fmt"`,
			`"github.com/acksell/bezos/dynamodb/ddbsdk"`,
			`"github.com/acksell/bezos/dynamodb/index"`,
			`"github.com/acksell/bezos/dynamodb/table"`,
		},
		Indexes: indexes,
	}

	// Execute template
	var buf bytes.Buffer
	tmpl, err := template.New("main").Funcs(templateFuncs).Parse(mainTemplate)
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	if err := tmpl.Execute(&buf, tmplData); err != nil {
		return nil, fmt.Errorf("executing template: %w", err)
	}

	// Format the generated code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		// Return unformatted code with error for debugging
		return buf.Bytes(), fmt.Errorf("formatting generated code: %w\n%s", err, buf.String())
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
	Name         string // Entity name (e.g., "User")
	IndexVarName string // Variable name (e.g., "userIndex")
	EntityType   string // Entity type name (e.g., "User")
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
	GSIs         []gsiData
	IsVersioned  bool // True if entity implements VersionedDynamoEntity
}

// HasEntity returns true if this index has an associated entity type.
func (d indexData) HasEntity() bool {
	return d.EntityType != ""
}

// keyData holds introspected data about a key pattern.
type keyData struct {
	Params           []paramData // Function parameters derived from the pattern
	FormatExpr       string      // Go expression to format the key value (using params)
	EntityFormatExpr string      // Go expression to format using entity fields
}

// paramData represents a function parameter.
type paramData struct {
	Name string
	Type string
}

// gsiData holds introspected data about a GSI.
type gsiData struct {
	Name         string
	Index        int
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
}

// buildIndexData converts an IndexInfo to indexData for the template.
func buildIndexData(idx IndexInfo, fields []FieldInfo) (indexData, error) {
	// Build tag-to-field mapping
	tagMap := make(map[string]FieldInfo)
	for _, f := range fields {
		tagMap[f.Tag] = f
	}

	pkData, err := buildKeyData(idx.PartitionKey, tagMap)
	if err != nil {
		return indexData{}, fmt.Errorf("partition key: %w", err)
	}

	data := indexData{
		Name:         idx.EntityType,
		IndexVarName: idx.VarName,
		EntityType:   idx.EntityType,
		PartitionKey: pkData,
		IsVersioned:  idx.IsVersioned,
	}

	if idx.SortKey != "" {
		skData, err := buildKeyData(idx.SortKey, tagMap)
		if err != nil {
			return indexData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	// Process GSIs
	for _, gsi := range idx.GSIs {
		gsiData, err := buildGSIData(gsi, tagMap)
		if err != nil {
			return indexData{}, fmt.Errorf("GSI %s: %w", gsi.Name, err)
		}
		data.GSIs = append(data.GSIs, gsiData)
	}

	return data, nil
}

// buildGSIData converts a GSIInfo to gsiData for the template.
func buildGSIData(gsi GSIInfo, tagMap map[string]FieldInfo) (gsiData, error) {
	pkData, err := buildKeyData(gsi.PKPattern, tagMap)
	if err != nil {
		return gsiData{}, fmt.Errorf("partition key: %w", err)
	}

	data := gsiData{
		Name:         gsi.Name,
		Index:        gsi.Index,
		PartitionKey: pkData,
	}

	if gsi.SKPattern != "" {
		skData, err := buildKeyData(gsi.SKPattern, tagMap)
		if err != nil {
			return gsiData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	return data, nil
}

// fieldRefRegex matches {fieldName} or {nested.field.path} patterns
var fieldRefRegex = regexp.MustCompile(`\{([^}]+)\}`)

// buildKeyData converts a pattern string to keyData for the template.
func buildKeyData(pattern string, tagMap map[string]FieldInfo) (keyData, error) {
	// Find all field references
	matches := fieldRefRegex.FindAllStringSubmatch(pattern, -1)

	if len(matches) == 0 {
		// Constant pattern - no parameters
		return keyData{
			Params:           nil,
			FormatExpr:       fmt.Sprintf("%q", pattern),
			EntityFormatExpr: fmt.Sprintf("%q", pattern),
		}, nil
	}

	var params []paramData
	var formatParts []string
	var entityFormatParts []string

	// Split the pattern into parts
	lastEnd := 0
	for _, loc := range fieldRefRegex.FindAllStringSubmatchIndex(pattern, -1) {
		start, end := loc[0], loc[1]
		fieldName := pattern[loc[2]:loc[3]]

		// Add literal before this field
		if start > lastEnd {
			literal := pattern[lastEnd:start]
			formatParts = append(formatParts, fmt.Sprintf("%q", literal))
			entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", literal))
		}

		// Add field reference
		// Use last component as param name (for nested fields like "user.id", use "id")
		pathParts := strings.Split(fieldName, ".")
		paramName := pathParts[len(pathParts)-1]
		params = append(params, paramData{Name: paramName, Type: "string"})
		formatParts = append(formatParts, paramName)

		// Find Go field from tag map
		if field, ok := tagMap[fieldName]; ok {
			entityFormatParts = append(entityFormatParts, "e."+field.Name)
		} else {
			return keyData{}, fmt.Errorf("no struct field found with tag %q", fieldName)
		}

		lastEnd = end
	}

	// Add trailing literal
	if lastEnd < len(pattern) {
		literal := pattern[lastEnd:]
		formatParts = append(formatParts, fmt.Sprintf("%q", literal))
		entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", literal))
	}

	// Build format expressions
	formatExpr := buildFormatExpr(params, formatParts)
	entityFormatExpr := buildFormatExprFromParts(entityFormatParts)

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

// buildFormatExprFromParts builds a Go expression from parts that are either
// quoted constants (e.g., `"USER#"`) or Go expressions (e.g., `e.UserID`).
func buildFormatExprFromParts(parts []string) string {
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

// {{$idx.Name}}IndexUtil wraps the PrimaryIndex with strongly-typed methods.
type {{$idx.Name}}IndexUtil struct {
	*index.PrimaryIndex[{{$idx.EntityType}}]
}

// {{$idx.Name}}Index is the typed wrapper for {{$idx.Name}} operations.
var {{$idx.Name}}Index = {{$idx.Name}}IndexUtil{PrimaryIndex: &{{$idx.IndexVarName}}}

// PrimaryKey creates a primary key from explicit parameters.
func (idx {{$idx.Name}}IndexUtil) PrimaryKey({{allParams $idx}}) table.PrimaryKey {
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
// PrimaryKeyFrom creates the primary key from a {{$idx.EntityType}} entity.
func (idx {{$idx.Name}}IndexUtil) PrimaryKeyFrom(e *{{$idx.EntityType}}) table.PrimaryKey {
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
{{if $idx.GSIs}}
// GSIKeysFrom creates all GSI keys from a {{$idx.EntityType}} entity.
func (idx {{$idx.Name}}IndexUtil) GSIKeysFrom(e *{{$idx.EntityType}}) []table.PrimaryKey {
	return []table.PrimaryKey{
		{{- range $gsi := $idx.GSIs}}
		{
			Definition: idx.Secondary[{{$gsi.Index}}].KeyDefinition(),
			Values: table.PrimaryKeyValues{
				PartitionKey: {{$gsi.PartitionKey.EntityFormatExpr}},
				{{- if $gsi.HasSortKey}}
				SortKey:      {{$gsi.SortKey.EntityFormatExpr}},
				{{- end}}
			},
		},
		{{- end}}
	}
}
{{end}}
// UnsafePut creates a Put operation without optimistic locking.
func (idx {{$idx.Name}}IndexUtil) UnsafePut(e *{{$idx.EntityType}}) *ddbsdk.Put {
	{{- if $idx.GSIs}}
	return ddbsdk.NewUnsafePut(idx.Table, idx.PrimaryKeyFrom(e), e).WithGSIKeys(idx.GSIKeysFrom(e)...)
	{{- else}}
	return ddbsdk.NewUnsafePut(idx.Table, idx.PrimaryKeyFrom(e), e)
	{{- end}}
}
{{if $idx.IsVersioned}}
// SafePut creates a Put operation with optimistic locking.
func (idx {{$idx.Name}}IndexUtil) SafePut(e *{{$idx.EntityType}}) *ddbsdk.PutWithCondition {
	{{- if $idx.GSIs}}
	return ddbsdk.NewSafePut(idx.Table, idx.PrimaryKeyFrom(e), e).WithGSIKeys(idx.GSIKeysFrom(e)...)
	{{- else}}
	return ddbsdk.NewSafePut(idx.Table, idx.PrimaryKeyFrom(e), e)
	{{- end}}
}
{{end}}
{{end}}
// Delete creates a Delete operation.
func (idx {{$idx.Name}}IndexUtil) Delete({{allParams $idx}}) *ddbsdk.Delete {
	return ddbsdk.NewDelete(idx.Table, idx.PrimaryKey({{allArgs $idx}}))
}

// UnsafeUpdate creates an Update operation without optimistic locking.
func (idx {{$idx.Name}}IndexUtil) UnsafeUpdate({{allParams $idx}}) *ddbsdk.UnsafeUpdate {
	return ddbsdk.NewUnsafeUpdate(idx.Table, idx.PrimaryKey({{allArgs $idx}}))
}

{{range $gsi := $idx.GSIs}}
// {{$gsi.Name}}Key creates a key for querying the {{$gsi.Name}} GSI.
func (idx {{$idx.Name}}IndexUtil) {{$gsi.Name}}Key({{gsiAllParams $gsi}}) table.PrimaryKey {
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
