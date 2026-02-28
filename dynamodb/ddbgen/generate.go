package ddbgen

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"go/format"
	"io"
	"os"
	"strings"
	"text/template"

	"github.com/acksell/bezos/dynamodb/index/val"
)

// GenerateOptions configures code generation behavior.
type GenerateOptions struct {
	// WarnWriter receives warning messages. Defaults to os.Stderr if nil.
	WarnWriter io.Writer
}

// Generate produces type-safe key constructor code from discovered indexes.
func Generate(result *DiscoverResult) ([]byte, error) {
	return GenerateWithOptions(result, GenerateOptions{WarnWriter: os.Stderr})
}

// GenerateWithOptions produces type-safe key constructor code with configurable options.
func GenerateWithOptions(result *DiscoverResult, opts GenerateOptions) ([]byte, error) {
	if opts.WarnWriter == nil {
		opts.WarnWriter = os.Stderr
	}

	if len(result.Indexes) == 0 {
		return nil, fmt.Errorf("no indexes to generate")
	}

	// Convert discovery result to template data
	indexes := make([]indexData, 0, len(result.Indexes))
	needsStrconv := false
	needsTime := false

	for _, idx := range result.Indexes {
		data, err := buildIndexData(idx, result.EntityFields[idx.EntityType], opts.WarnWriter)
		if err != nil {
			return nil, fmt.Errorf("building data for %s: %w", idx.VarName, err)
		}
		indexes = append(indexes, data)

		// Check if we need strconv import
		if needsStrconvImport(data) {
			needsStrconv = true
		}
		if needsTimeImport(data) {
			needsTime = true
		}
	}

	imports := []string{
		`"fmt"`,
		`"github.com/acksell/bezos/dynamodb/ddbsdk"`,
		`"github.com/acksell/bezos/dynamodb/index"`,
		`"github.com/acksell/bezos/dynamodb/table"`,
	}
	if needsStrconv {
		imports = append([]string{`"strconv"`}, imports...)
	}
	if needsTime {
		imports = append([]string{`"time"`}, imports...)
	}

	tmplData := templateData{
		Package: result.PackageName,
		Imports: imports,
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
	IsConstant       bool        // True if the key has no field references
	LiteralPrefix    string      // Leading literal portion before first field ref (for BeginsWith)
	FieldRefNames    []string    // Field reference names in the pattern (e.g., ["orderID"])
	UsesStrconv      bool        // True if strconv package is needed
	UsesTime         bool        // True if time package is needed
}

// paramData represents a function parameter.
type paramData struct {
	Name       string
	Type       string
	FieldType  string   // Original Go type from struct (e.g., "int64", "time.Time")
	Formats    []string // Format modifiers (e.g., ["utc", "rfc3339"] or ["unixnano"])
	PrintfSpec string   // Printf spec for padded formats (e.g., "%020d")
}

// gsiData holds introspected data about a GSI.
type gsiData struct {
	Name         string
	Index        int
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
}

// needsStrconvImport checks if any key in the index needs strconv import.
func needsStrconvImport(idx indexData) bool {
	if idx.PartitionKey.UsesStrconv {
		return true
	}
	if idx.SortKey != nil && idx.SortKey.UsesStrconv {
		return true
	}
	for _, gsi := range idx.GSIs {
		if gsi.PartitionKey.UsesStrconv {
			return true
		}
		if gsi.SortKey != nil && gsi.SortKey.UsesStrconv {
			return true
		}
	}
	return false
}

// needsTimeImport checks if any key in the index needs time import.
func needsTimeImport(idx indexData) bool {
	if idx.PartitionKey.UsesTime {
		return true
	}
	if idx.SortKey != nil && idx.SortKey.UsesTime {
		return true
	}
	for _, gsi := range idx.GSIs {
		if gsi.PartitionKey.UsesTime {
			return true
		}
		if gsi.SortKey != nil && gsi.SortKey.UsesTime {
			return true
		}
	}
	return false
}

// buildIndexData converts an IndexInfo to indexData for the template.
func buildIndexData(idx IndexInfo, fields []FieldInfo, warnWriter io.Writer) (indexData, error) {
	// Build tag-to-field mapping
	tagMap := make(map[string]FieldInfo)
	for _, f := range fields {
		tagMap[f.Tag] = f
	}

	pkData, err := buildKeyData(idx.PartitionKey, tagMap, false, idx.EntityType, warnWriter)
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

	if idx.SortKey.Pattern != "" {
		skData, err := buildKeyData(idx.SortKey, tagMap, true, idx.EntityType, warnWriter)
		if err != nil {
			return indexData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	// Process GSIs
	for _, gsi := range idx.GSIs {
		gsiData, err := buildGSIData(gsi, tagMap, idx.EntityType, warnWriter)
		if err != nil {
			return indexData{}, fmt.Errorf("GSI %s: %w", gsi.Name, err)
		}
		data.GSIs = append(data.GSIs, gsiData)
	}

	return data, nil
}

// buildGSIData converts a GSIInfo to gsiData for the template.
func buildGSIData(gsi GSIInfo, tagMap map[string]FieldInfo, entityType string, warnWriter io.Writer) (gsiData, error) {
	pkData, err := buildKeyData(gsi.PKPattern, tagMap, false, entityType, warnWriter)
	if err != nil {
		return gsiData{}, fmt.Errorf("partition key: %w", err)
	}

	data := gsiData{
		Name:         gsi.Name,
		Index:        gsi.Index,
		PartitionKey: pkData,
	}

	if gsi.SKPattern.Pattern != "" {
		skData, err := buildKeyData(gsi.SKPattern, tagMap, true, entityType, warnWriter)
		if err != nil {
			return gsiData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	return data, nil
}

// buildKeyData converts a key pattern to keyData for the template.
func buildKeyData(kp KeyPattern, tagMap map[string]FieldInfo, isSortKey bool, entityType string, warnWriter io.Writer) (keyData, error) {
	pattern := kp.Pattern

	// Parse the pattern using val.FmtSpec
	spec, err := val.ParseFmt(pattern)
	if err != nil {
		return keyData{}, fmt.Errorf("invalid pattern %q: %w", pattern, err)
	}

	if spec.IsConstant() {
		// Constant pattern - no parameters
		formatExpr := fmt.Sprintf("%q", pattern)

		// For bytes, decode base64 and generate a byte slice literal
		if kp.Kind == KeyKindBytes {
			decoded, err := base64.StdEncoding.DecodeString(pattern)
			if err != nil {
				return keyData{}, fmt.Errorf("invalid base64 for bytes key: %w", err)
			}
			formatExpr = formatByteLiteral(decoded)
		}

		return keyData{
			Params:           nil,
			FormatExpr:       formatExpr,
			EntityFormatExpr: formatExpr,
			IsConstant:       true,
			LiteralPrefix:    pattern,
		}, nil
	}

	var params []paramData
	var formatParts []string
	var entityFormatParts []string
	var fieldRefNames []string
	usesStrconv := false
	usesTime := false

	literalPrefix := spec.LiteralPrefix()

	// Process each part of the parsed spec
	for _, part := range spec.Parts {
		if part.IsLiteral {
			formatParts = append(formatParts, fmt.Sprintf("%q", part.Value))
			entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", part.Value))
			continue
		}

		// Field reference
		fieldPath := part.Value

		// Find Go field from tag map
		field, ok := tagMap[fieldPath]
		if !ok {
			return keyData{}, fmt.Errorf("no struct field found with tag %q", fieldPath)
		}

		// Get param name from the SpecPart
		paramName := part.ParamName()
		paramType := part.GoParamType(field.Type)

		// Track if we need time import for parameter type
		if paramType == "time.Time" {
			usesTime = true
		}

		params = append(params, paramData{
			Name:       paramName,
			Type:       paramType,
			FieldType:  field.Type,
			Formats:    part.Formats,
			PrintfSpec: part.PrintfSpec,
		})

		// Emit sort key warning if applicable
		if isSortKey {
			if warning := part.SortKeyWarning(field.Type, entityType); warning != "" {
				fmt.Fprint(warnWriter, warning)
			}
		}

		// Generate param format expression
		paramResult, err := part.GenerateConversionExpr(paramName, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", fieldPath, err)
		}
		formatParts = append(formatParts, paramResult.Expr)
		fieldRefNames = append(fieldRefNames, paramName)
		usesStrconv = usesStrconv || paramResult.UsesStrconv
		usesTime = usesTime || paramResult.UsesTime

		// Generate entity format expression
		entityFieldExpr := "e." + field.Name
		entityResult, err := part.GenerateConversionExpr(entityFieldExpr, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", fieldPath, err)
		}
		entityFormatParts = append(entityFormatParts, entityResult.Expr)
		usesStrconv = usesStrconv || entityResult.UsesStrconv
		usesTime = usesTime || entityResult.UsesTime
	}

	// Build format expressions
	formatExpr := buildTypedFormatExpr(formatParts)
	entityFormatExpr := buildTypedFormatExpr(entityFormatParts)

	return keyData{
		Params:           params,
		FormatExpr:       formatExpr,
		EntityFormatExpr: entityFormatExpr,
		IsConstant:       false,
		LiteralPrefix:    literalPrefix,
		FieldRefNames:    fieldRefNames,
		UsesStrconv:      usesStrconv,
		UsesTime:         usesTime,
	}, nil
}

// buildTypedFormatExpr builds a Go expression from parts that are either
// quoted constants or conversion expressions.
func buildTypedFormatExpr(parts []string) string {
	if len(parts) == 0 {
		return `""`
	}
	if len(parts) == 1 {
		p := parts[0]
		// If it's a quoted string, return as-is
		if strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`) {
			return p
		}
		// Otherwise it's an expression that returns string
		return p
	}

	// Multiple parts - need to concatenate
	// Check if all parts are simple string values or expressions
	var resultParts []string
	for _, p := range parts {
		if strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`) {
			// Quoted constant
			resultParts = append(resultParts, p)
		} else {
			// Expression
			resultParts = append(resultParts, p)
		}
	}

	// Use string concatenation
	return strings.Join(resultParts, " + ")
}

// formatByteLiteral generates a Go byte slice literal like []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}.
func formatByteLiteral(data []byte) string {
	if len(data) == 0 {
		return "[]byte{}"
	}
	var parts []string
	for _, b := range data {
		parts = append(parts, fmt.Sprintf("0x%02x", b))
	}
	return "[]byte{" + strings.Join(parts, ", ") + "}"
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
	// pkParams returns only the partition key params as a function signature fragment.
	"pkParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	// pkArgs returns only the partition key params as call arguments.
	"pkArgs": func(kd keyData) string {
		var args []string
		for _, p := range kd.Params {
			args = append(args, p.Name)
		}
		return strings.Join(args, ", ")
	},
	// skPrefix returns the method name prefix based on sort key field refs.
	// Single field ref: returns the TitleCase field name (e.g., "OrderID").
	// Multiple field refs: returns "SK".
	"skPrefix": func(kd keyData) string {
		if len(kd.FieldRefNames) == 1 {
			name := kd.FieldRefNames[0]
			if len(name) == 0 {
				return "SK"
			}
			return strings.ToUpper(name[:1]) + name[1:]
		}
		return "SK"
	},
	// skEqualsFormatExpr returns a Go expression that formats the sort key for an Equals condition.
	"skEqualsFormatExpr": func(kd keyData) string {
		return kd.FormatExpr
	},
	// skBeginsWithExpr returns a Go expression for a BeginsWith prefix.
	// For "ORDER#{orderID}", returns: "ORDER#" + prefix
	"skBeginsWithExpr": func(kd keyData) string {
		if kd.LiteralPrefix != "" {
			return fmt.Sprintf("%q + prefix", kd.LiteralPrefix)
		}
		return "prefix"
	},
	// skBetweenStartExpr returns a Go expression formatting the start value for Between.
	"skBetweenStartExpr": func(kd keyData) string {
		return buildSKBoundExpr(kd, "Start")
	},
	// skBetweenEndExpr returns a Go expression formatting the end value for Between.
	"skBetweenEndExpr": func(kd keyData) string {
		return buildSKBoundExpr(kd, "End")
	},
	// skBoundExpr returns a Go expression formatting a single-value SK condition arg.
	"skBoundExpr": func(kd keyData) string {
		return buildSKBoundExpr(kd, "")
	},
	// skBeginsWithParams returns the BeginsWith function parameters for this sort key.
	"skBeginsWithParams": func(kd keyData) string {
		return "prefix string"
	},
	// skEqualsParams returns the Equals function parameters for this sort key.
	"skEqualsParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	// skBetweenParams returns the Between function parameters for this sort key.
	"skBetweenParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%sStart %s, %sEnd %s", p.Name, p.Type, p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	// skSingleValueParams returns params for single-value SK conditions (GT, GTE, LT, LTE).
	"skSingleValueParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
}

// buildSKBoundExpr builds a Go expression for a sort key bound (used in Between, GT, LT, etc.).
// The suffix is appended to param names (e.g., "Start" for Between start values).
func buildSKBoundExpr(kd keyData, suffix string) string {
	if len(kd.Params) == 0 {
		return kd.FormatExpr
	}

	// For non-string types, we need to generate proper conversion expressions
	// using the same logic as the primary key generation
	if len(kd.Params) == 1 {
		p := kd.Params[0]
		paramName := p.Name + suffix

		// Create a val.SpecPart to use its GenerateConversionExpr method
		specPart := val.SpecPart{
			Value:      p.Name,
			Formats:    p.Formats,
			PrintfSpec: p.PrintfSpec,
		}

		result, err := specPart.GenerateConversionExpr(paramName, p.FieldType)
		if err != nil {
			// Fall back to string conversion for safety
			return fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", paramName)
		}

		expr := result.Expr
		if kd.LiteralPrefix != "" {
			return fmt.Sprintf("%q + %s", kd.LiteralPrefix, expr)
		}
		return expr
	}

	// Multiple params: build the full expression with proper conversions
	var parts []string
	if kd.LiteralPrefix != "" {
		parts = append(parts, fmt.Sprintf("%q", kd.LiteralPrefix))
	}

	for _, p := range kd.Params {
		paramName := p.Name + suffix
		specPart := val.SpecPart{
			Value:      p.Name,
			Formats:    p.Formats,
			PrintfSpec: p.PrintfSpec,
		}

		result, err := specPart.GenerateConversionExpr(paramName, p.FieldType)
		if err != nil {
			parts = append(parts, fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", paramName))
		} else {
			parts = append(parts, result.Expr)
		}
	}

	return strings.Join(parts, " + ")
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

// -------------------------------------------------------------------------
// Primary Index Query Builder
// -------------------------------------------------------------------------

// {{$idx.Name}}PrimaryQuery is a query builder for the primary index.
type {{$idx.Name}}PrimaryQuery struct {
	idx *{{$idx.Name}}IndexUtil
	qd  ddbsdk.QueryDef
}

// Build returns the underlying QueryDef, implementing ddbsdk.QueryBuilder.
func (q {{$idx.Name}}PrimaryQuery) Build() ddbsdk.QueryDef { return q.qd }

// QueryPartition creates a query for the given partition key on the primary index.
func (idx {{$idx.Name}}IndexUtil) QueryPartition({{pkParams $idx.PartitionKey}}) {{$idx.Name}}PrimaryQuery {
	return {{$idx.Name}}PrimaryQuery{
		idx: &idx,
		qd:  ddbsdk.QueryPartition(idx.Table, {{$idx.PartitionKey.FormatExpr}}),
	}
}
{{if and $idx.HasSortKey (not $idx.SortKey.IsConstant)}}
// {{skPrefix $idx.SortKey}}Equals adds a sort key equals condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}Equals({{skEqualsParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.Equals({{skEqualsFormatExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}BeginsWith adds a sort key begins_with condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}BeginsWith({{skBeginsWithParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.BeginsWith({{skBeginsWithExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}Between adds a sort key between condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}Between({{skBetweenParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.Between({{skBetweenStartExpr $idx.SortKey}}, {{skBetweenEndExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}GreaterThan adds a sort key > condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}GreaterThan({{skSingleValueParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.GreaterThan({{skBoundExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}GreaterThanOrEqual adds a sort key >= condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}GreaterThanOrEqual({{skSingleValueParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.GreaterThanOrEqual({{skBoundExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}LessThan adds a sort key < condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}LessThan({{skSingleValueParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.LessThan({{skBoundExpr $idx.SortKey}}))
}

// {{skPrefix $idx.SortKey}}LessThanOrEqual adds a sort key <= condition and returns the final QueryDef.
func (q {{$idx.Name}}PrimaryQuery) {{skPrefix $idx.SortKey}}LessThanOrEqual({{skSingleValueParams $idx.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.LessThanOrEqual({{skBoundExpr $idx.SortKey}}))
}
{{end}}
{{range $gsi := $idx.GSIs}}
// -------------------------------------------------------------------------
// {{$idx.Name}}Index{{$gsi.Name}} - Query-only GSI Wrapper
// -------------------------------------------------------------------------

// {{$idx.Name}}Index{{$gsi.Name}}Util provides query methods for the {{$gsi.Name}} GSI.
type {{$idx.Name}}Index{{$gsi.Name}}Util struct {
	primary *{{$idx.Name}}IndexUtil
}

// {{$idx.Name}}Index{{$gsi.Name}} is the query-only wrapper for the {{$gsi.Name}} GSI.
var {{$idx.Name}}Index{{$gsi.Name}} = {{$idx.Name}}Index{{$gsi.Name}}Util{primary: &{{$idx.Name}}Index}

// {{$idx.Name}}{{$gsi.Name}}Query is a query builder for the {{$gsi.Name}} GSI.
type {{$idx.Name}}{{$gsi.Name}}Query struct {
	idx *{{$idx.Name}}Index{{$gsi.Name}}Util
	qd  ddbsdk.QueryDef
}

// QueryDef returns the underlying QueryDef, implementing ddbsdk.QueryDefinition.
func (q {{$idx.Name}}{{$gsi.Name}}Query) QueryDef() ddbsdk.QueryDef { return q.qd }

// QueryPartition creates a query for the given partition key on the {{$gsi.Name}} GSI.
func (idx {{$idx.Name}}Index{{$gsi.Name}}Util) QueryPartition({{pkParams $gsi.PartitionKey}}) {{$idx.Name}}{{$gsi.Name}}Query {
	return {{$idx.Name}}{{$gsi.Name}}Query{
		idx: &idx,
		qd:  ddbsdk.QueryPartition(idx.primary.Table, {{$gsi.PartitionKey.FormatExpr}}).OnIndex("{{$gsi.Name}}"),
	}
}
{{if and $gsi.HasSortKey (not $gsi.SortKey.IsConstant)}}
// {{skPrefix $gsi.SortKey}}Equals adds a sort key equals condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}Equals({{skEqualsParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.Equals({{skEqualsFormatExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}BeginsWith adds a sort key begins_with condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}BeginsWith({{skBeginsWithParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.BeginsWith({{skBeginsWithExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}Between adds a sort key between condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}Between({{skBetweenParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.Between({{skBetweenStartExpr $gsi.SortKey}}, {{skBetweenEndExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}GreaterThan adds a sort key > condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}GreaterThan({{skSingleValueParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.GreaterThan({{skBoundExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}GreaterThanOrEqual adds a sort key >= condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}GreaterThanOrEqual({{skSingleValueParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.GreaterThanOrEqual({{skBoundExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}LessThan adds a sort key < condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}LessThan({{skSingleValueParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.LessThan({{skBoundExpr $gsi.SortKey}}))
}

// {{skPrefix $gsi.SortKey}}LessThanOrEqual adds a sort key <= condition and returns the final QueryDef.
func (q {{$idx.Name}}{{$gsi.Name}}Query) {{skPrefix $gsi.SortKey}}LessThanOrEqual({{skSingleValueParams $gsi.SortKey}}) ddbsdk.QueryDef {
	return q.qd.WithSKCondition(ddbsdk.LessThanOrEqual({{skBoundExpr $gsi.SortKey}}))
}
{{end}}
{{end}}
{{end}}
`
