package ddbgen

import (
	"bytes"
	"encoding/base64"
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
	IsConstant       bool        // True if the key has no field references
	LiteralPrefix    string      // Leading literal portion before first field ref (for BeginsWith)
	FieldRefNames    []string    // Field reference names in the pattern (e.g., ["orderID"])
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

	if idx.SortKey.Pattern != "" {
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

	if gsi.SKPattern.Pattern != "" {
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

// buildKeyData converts a key pattern to keyData for the template.
func buildKeyData(kp KeyPattern, tagMap map[string]FieldInfo) (keyData, error) {
	pattern := kp.Pattern
	// Find all field references
	matches := fieldRefRegex.FindAllStringSubmatch(pattern, -1)

	if len(matches) == 0 {
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

	// Extract literal prefix (everything before the first field reference)
	firstMatchLocs := fieldRefRegex.FindStringSubmatchIndex(pattern)
	literalPrefix := ""
	if firstMatchLocs != nil && firstMatchLocs[0] > 0 {
		literalPrefix = pattern[:firstMatchLocs[0]]
	}

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
		fieldRefNames = append(fieldRefNames, paramName)

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
		IsConstant:       false,
		LiteralPrefix:    literalPrefix,
		FieldRefNames:    fieldRefNames,
	}, nil
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
func buildSKBoundExpr(kd keyData, suffix string) string {
	if len(kd.Params) == 0 {
		return kd.FormatExpr
	}
	if len(kd.Params) == 1 {
		paramName := kd.Params[0].Name + suffix
		if kd.LiteralPrefix != "" {
			return fmt.Sprintf("fmt.Sprintf(%q, %s)", kd.LiteralPrefix+"%s", paramName)
		}
		return paramName
	}
	// Multiple params: use the full format pattern with suffixed param names
	var fmtParts []string
	var args []string
	fmtParts = append(fmtParts, kd.LiteralPrefix)
	for i, p := range kd.Params {
		args = append(args, p.Name+suffix)
		if i > 0 {
			// Add inter-field literal separators — but we don't have them easily.
			// Fall back to the format expression with suffixed args.
		}
		fmtParts = append(fmtParts, "%s")
	}
	return fmt.Sprintf("fmt.Sprintf(%q, %s)", strings.Join(fmtParts, ""), strings.Join(args, ", "))
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
