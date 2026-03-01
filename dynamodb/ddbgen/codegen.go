package ddbgen

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"go/format"
	"os"
	"strings"
	"text/template"

	"github.com/acksell/bezos/dynamodb/index/val"
)

// =============================================================================
// Conversion expression generation
// =============================================================================

type conversionResult struct {
	Expr        string
	UsesFmt     bool
	UsesStrconv bool
	UsesTime    bool
}

func generateConversionExpr(p val.SpecPart, fieldExpr string, fieldType string) (conversionResult, error) {
	if fieldType == "string" {
		if p.PrintfSpec != "" {
			return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", p.PrintfSpec, fieldExpr), UsesFmt: true}, nil
		}
		return conversionResult{Expr: fieldExpr}, nil
	}
	if isIntegerType(fieldType) {
		if p.PrintfSpec != "" {
			return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", p.PrintfSpec, fieldExpr), UsesFmt: true}, nil
		}
		if isSignedIntegerType(fieldType) {
			if fieldType == "int64" {
				return conversionResult{Expr: fmt.Sprintf("strconv.FormatInt(%s, 10)", fieldExpr), UsesStrconv: true}, nil
			}
			return conversionResult{Expr: fmt.Sprintf("strconv.FormatInt(int64(%s), 10)", fieldExpr), UsesStrconv: true}, nil
		}
		if fieldType == "uint64" {
			return conversionResult{Expr: fmt.Sprintf("strconv.FormatUint(%s, 10)", fieldExpr), UsesStrconv: true}, nil
		}
		return conversionResult{Expr: fmt.Sprintf("strconv.FormatUint(uint64(%s), 10)", fieldExpr), UsesStrconv: true}, nil
	}
	if isFloatType(fieldType) {
		format := p.Format()
		if p.PrintfSpec == "" && format == "" {
			return conversionResult{}, fmt.Errorf("float type %s requires explicit format", fieldType)
		}
		spec := p.PrintfSpec
		if spec == "" {
			spec = format
		}
		return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", spec, fieldExpr), UsesFmt: true}, nil
	}
	if isTimeType(fieldType) {
		format := p.Format()
		if format == "" {
			return conversionResult{}, fmt.Errorf("time.Time field requires explicit format")
		}
		timeExpr := fieldExpr
		if p.HasModifier("utc") {
			timeExpr = fmt.Sprintf("%s.UTC()", fieldExpr)
		}
		switch format {
		case "unix":
			if p.PrintfSpec != "" {
				return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.Unix())", p.PrintfSpec, timeExpr), UsesFmt: true}, nil
			}
			return conversionResult{Expr: fmt.Sprintf("strconv.FormatInt(%s.Unix(), 10)", timeExpr), UsesStrconv: true}, nil
		case "unixmilli":
			if p.PrintfSpec != "" {
				return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.UnixMilli())", p.PrintfSpec, timeExpr), UsesFmt: true}, nil
			}
			return conversionResult{Expr: fmt.Sprintf("strconv.FormatInt(%s.UnixMilli(), 10)", timeExpr), UsesStrconv: true}, nil
		case "unixnano":
			if p.PrintfSpec != "" {
				return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.UnixNano())", p.PrintfSpec, timeExpr), UsesFmt: true}, nil
			}
			return conversionResult{Expr: fmt.Sprintf("strconv.FormatInt(%s.UnixNano(), 10)", timeExpr), UsesStrconv: true}, nil
		case "rfc3339":
			return conversionResult{Expr: fmt.Sprintf("%s.Format(time.RFC3339)", timeExpr), UsesTime: true}, nil
		case "rfc3339fixed":
			return conversionResult{Expr: fmt.Sprintf("%s.Format(%q)", timeExpr, "2006-01-02T15:04:05.000000000Z07:00")}, nil
		case "rfc3339nano":
			return conversionResult{Expr: fmt.Sprintf("%s.Format(time.RFC3339Nano)", timeExpr), UsesTime: true}, nil
		case "utc":
			return conversionResult{}, fmt.Errorf("time.Time field with :utc modifier requires a format")
		default:
			return conversionResult{Expr: fmt.Sprintf("%s.Format(%q)", timeExpr, format)}, nil
		}
	}
	return conversionResult{Expr: fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", fieldExpr), UsesFmt: true}, nil
}

func sortKeyWarning(p val.SpecPart, fieldType string, entityType string) string {
	if isIntegerType(fieldType) && !hasPadding(p.PrintfSpec) {
		return fmt.Sprintf("warning: %s sort key uses %s without padding format.\n"+
			"  String comparison treats \"9\" > \"10\". For correct ordering either:\n"+
			"  - Use DynamoDB Number type for the sort key\n"+
			"  - Add zero-padding: {field:%%020d}\n", entityType, fieldType)
	}
	if isFloatType(fieldType) && !hasPadding(p.PrintfSpec) {
		spec := p.PrintfSpec
		if spec == "" {
			spec = p.Format()
		}
		return fmt.Sprintf("warning: %s sort key uses %s format %q without total width padding.\n"+
			"  For correct string sorting, specify total width: {field:%%020.2f}\n", entityType, fieldType, spec)
	}
	format := p.Format()
	if isTimeType(fieldType) {
		switch format {
		case "unix":
			if !hasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unix\" timestamp without padding.\n"+
					"  Unix timestamps change digit count (9 digits before 2001-09-09, 10 after).\n"+
					"  For correct string sorting, add padding: {field:unix:%%011d}\n", entityType)
			}
		case "unixmilli":
			if !hasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unixmilli\" timestamp without padding.\n"+
					"  Unix millisecond timestamps change digit count (12 digits before 2001-09-09, 13 after).\n"+
					"  For correct string sorting, add padding: {field:unixmilli:%%014d}\n", entityType)
			}
		case "unixnano":
			if !hasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unixnano\" timestamp without padding.\n"+
					"  Unix nanosecond timestamps change digit count (18 digits before 2001-09-09, 19 after).\n"+
					"  For correct string sorting, add padding: {field:unixnano:%%020d}\n", entityType)
			}
		case "rfc3339":
			return fmt.Sprintf("warning: %s sort key uses \"rfc3339\" time format.\n"+
				"  RFC3339 has variable-width timezone offsets (+00:00 vs Z vs +05:30), making\n"+
				"  string comparison unreliable for ordering across timezones.\n"+
				"  For correct string ordering, prefer:\n"+
				"  - unix, unixmilli, or unixnano with padding (numeric, timezone-independent)\n"+
				"  - {field:utc:rfc3339fixed} to normalize to UTC with constant length\n", entityType)
		case "rfc3339fixed":
			if !p.HasModifier("utc") {
				return fmt.Sprintf("warning: %s sort key uses \"rfc3339fixed\" without :utc modifier.\n"+
					"  While rfc3339fixed has constant length, different timezone offsets still\n"+
					"  cause incorrect string ordering (e.g., \"...+05:00\" > \"...Z\").\n"+
					"  For correct ordering, either:\n"+
					"  - Use {field:utc:rfc3339fixed} to normalize to UTC\n"+
					"  - Use unix/unixmilli/unixnano with padding (timezone-independent)\n", entityType)
			}
		case "rfc3339nano":
			return fmt.Sprintf("warning: %s sort key uses \"rfc3339nano\" time format.\n"+
				"  RFC3339Nano has variable length (trailing zeros stripped) and timezone issues.\n"+
				"  For correct string ordering, prefer:\n"+
				"  - unix, unixmilli, or unixnano with padding\n"+
				"  - {field:utc:rfc3339fixed} (constant length, UTC-normalized)\n", entityType)
		}
	}
	return ""
}

// =============================================================================
// Index data building
// =============================================================================

func buildIndexData(idx IndexInfo) (indexData, error) {
	tagMap := make(map[string]FieldInfo)
	for _, f := range idx.Fields {
		tagMap[f.Tag] = f
	}

	pkData, err := buildKeyData(idx.PartitionKey, tagMap, false, idx.EntityType)
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

	if idx.SortKey != nil && !idx.SortKey.IsZero() {
		skData, err := buildKeyData(*idx.SortKey, tagMap, true, idx.EntityType)
		if err != nil {
			return indexData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	for _, gsi := range idx.GSIs {
		gd, err := buildGSIData(gsi, tagMap, idx.EntityType)
		if err != nil {
			return indexData{}, fmt.Errorf("GSI %s: %w", gsi.Name, err)
		}
		data.GSIs = append(data.GSIs, gd)
	}

	return data, nil
}

func buildGSIData(gsi GSIInfo, tagMap map[string]FieldInfo, entityType string) (gsiData, error) {
	pkData, err := buildKeyData(gsi.PKPattern, tagMap, false, entityType)
	if err != nil {
		return gsiData{}, fmt.Errorf("partition key: %w", err)
	}

	data := gsiData{
		Name:         gsi.Name,
		Index:        gsi.Index,
		PartitionKey: pkData,
	}

	if gsi.SKPattern != nil && !gsi.SKPattern.IsZero() {
		skData, err := buildKeyData(*gsi.SKPattern, tagMap, true, entityType)
		if err != nil {
			return gsiData{}, fmt.Errorf("sort key: %w", err)
		}
		data.HasSortKey = true
		data.SortKey = &skData
	}

	return data, nil
}

func buildKeyData(vd val.ValDef, tagMap map[string]FieldInfo, isSortKey bool, entityType string) (keyData, error) {
	// Handle constant values
	if vd.Const != nil {
		kind := vd.Const.Kind
		var formatExpr string
		var constVal string
		switch kind {
		case val.SpecKindB:
			// After JSON round-trip, []byte becomes a base64 string
			// todo verify this, potential ai slop (well, more than it already is)
			switch v := vd.Const.Value.(type) {
			case []byte:
				formatExpr = formatByteLiteral(v)
				constVal = base64.StdEncoding.EncodeToString(v)
			case string:
				decoded, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					return keyData{}, fmt.Errorf("invalid base64 for bytes constant: %w", err)
				}
				formatExpr = formatByteLiteral(decoded)
				constVal = v
			default:
				return keyData{}, fmt.Errorf("unexpected type %T for bytes constant", vd.Const.Value)
			}
		case val.SpecKindN:
			constVal = fmt.Sprintf("%v", vd.Const.Value)
			formatExpr = constVal // Number constants are just literals
		default: // SpecKindS
			constVal = fmt.Sprintf("%v", vd.Const.Value)
			formatExpr = fmt.Sprintf("%q", constVal)
		}
		return keyData{
			FormatExpr:       formatExpr,
			EntityFormatExpr: formatExpr,
			IsConstant:       true,
			LiteralPrefix:    constVal,
		}, nil
	}

	// Handle FromField - direct field reference
	if vd.FromField != "" {
		field, ok := tagMap[vd.FromField]
		if !ok {
			return keyData{}, fmt.Errorf("no struct field found with tag %q", vd.FromField)
		}
		pName := vd.FromField
		pType := goParamType(field.Type)
		part := val.SpecPart{Value: vd.FromField}
		paramResult, err := generateConversionExpr(part, pName, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", vd.FromField, err)
		}
		entityResult, err := generateConversionExpr(part, "e."+field.Name, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", vd.FromField, err)
		}
		return keyData{
			Params:           []paramData{{Name: pName, Type: pType, FieldType: field.Type}},
			FormatExpr:       paramResult.Expr,
			EntityFormatExpr: entityResult.Expr,
			FieldRefNames:    []string{pName},
			UsesFmt:          paramResult.UsesFmt || entityResult.UsesFmt,
			UsesStrconv:      paramResult.UsesStrconv || entityResult.UsesStrconv,
			UsesTime:         pType == "time.Time" || paramResult.UsesTime || entityResult.UsesTime,
		}, nil
	}

	// Handle Format pattern
	if vd.Format == nil {
		return keyData{}, fmt.Errorf("ValDef has no value source")
	}
	spec := vd.Format

	if spec.IsConstant() {
		formatExpr := fmt.Sprintf("%q", spec.Raw)
		if spec.Kind == val.SpecKindB {
			decoded, err := base64.StdEncoding.DecodeString(spec.Raw)
			if err != nil {
				return keyData{}, fmt.Errorf("invalid base64 for bytes key: %w", err)
			}
			formatExpr = formatByteLiteral(decoded)
		}
		return keyData{
			FormatExpr:       formatExpr,
			EntityFormatExpr: formatExpr,
			IsConstant:       true,
			LiteralPrefix:    spec.Raw,
		}, nil
	}

	var params []paramData
	var formatParts, entityFormatParts []string
	var fieldRefNames []string
	usesStrconv, usesTime, usesFmt := false, false, false
	literalPrefix := spec.LiteralPrefix()

	for _, part := range spec.Parts {
		if part.IsLiteral {
			formatParts = append(formatParts, fmt.Sprintf("%q", part.Value))
			entityFormatParts = append(entityFormatParts, fmt.Sprintf("%q", part.Value))
			continue
		}
		field, ok := tagMap[part.Value]
		if !ok {
			return keyData{}, fmt.Errorf("no struct field found with tag %q", part.Value)
		}
		pName := part.ParamName()
		pType := goParamType(field.Type)
		if pType == "time.Time" {
			usesTime = true
		}
		params = append(params, paramData{
			Name: pName, Type: pType, FieldType: field.Type,
			Formats: part.Formats, PrintfSpec: part.PrintfSpec,
		})
		if isSortKey {
			if w := sortKeyWarning(part, field.Type, entityType); w != "" {
				fmt.Fprint(os.Stderr, w)
			}
		}
		paramResult, err := generateConversionExpr(part, pName, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", part.Value, err)
		}
		formatParts = append(formatParts, paramResult.Expr)
		fieldRefNames = append(fieldRefNames, pName)
		usesStrconv = usesStrconv || paramResult.UsesStrconv
		usesTime = usesTime || paramResult.UsesTime
		usesFmt = usesFmt || paramResult.UsesFmt

		entityResult, err := generateConversionExpr(part, "e."+field.Name, field.Type)
		if err != nil {
			return keyData{}, fmt.Errorf("field %q: %w", part.Value, err)
		}
		entityFormatParts = append(entityFormatParts, entityResult.Expr)
		usesStrconv = usesStrconv || entityResult.UsesStrconv
		usesTime = usesTime || entityResult.UsesTime
		usesFmt = usesFmt || entityResult.UsesFmt
	}

	return keyData{
		Params:           params,
		FormatExpr:       joinExprParts(formatParts),
		EntityFormatExpr: joinExprParts(entityFormatParts),
		LiteralPrefix:    literalPrefix,
		FieldRefNames:    fieldRefNames,
		UsesFmt:          usesFmt,
		UsesStrconv:      usesStrconv,
		UsesTime:         usesTime,
	}, nil
}

func joinExprParts(parts []string) string {
	if len(parts) == 0 {
		return `""`
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, " + ")
}

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

func buildSKBoundExpr(kd keyData, suffix string) string {
	if len(kd.Params) == 0 {
		return kd.FormatExpr
	}
	if len(kd.Params) == 1 {
		p := kd.Params[0]
		paramName := p.Name + suffix
		sp := val.SpecPart{Value: p.Name, Formats: p.Formats, PrintfSpec: p.PrintfSpec}
		result, err := generateConversionExpr(sp, paramName, p.FieldType)
		if err != nil {
			return fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", paramName)
		}
		if kd.LiteralPrefix != "" {
			return fmt.Sprintf("%q + %s", kd.LiteralPrefix, result.Expr)
		}
		return result.Expr
	}
	var parts []string
	if kd.LiteralPrefix != "" {
		parts = append(parts, fmt.Sprintf("%q", kd.LiteralPrefix))
	}
	for _, p := range kd.Params {
		paramName := p.Name + suffix
		sp := val.SpecPart{Value: p.Name, Formats: p.Formats, PrintfSpec: p.PrintfSpec}
		result, err := generateConversionExpr(sp, paramName, p.FieldType)
		if err != nil {
			parts = append(parts, fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", paramName))
		} else {
			parts = append(parts, result.Expr)
		}
	}
	return strings.Join(parts, " + ")
}

// =============================================================================
// Import detection
// =============================================================================

func needsFmtImport(idx indexData) bool {
	if idx.PartitionKey.UsesFmt {
		return true
	}
	if idx.SortKey != nil && idx.SortKey.UsesFmt {
		return true
	}
	for _, gsi := range idx.GSIs {
		if gsi.PartitionKey.UsesFmt {
			return true
		}
		if gsi.SortKey != nil && gsi.SortKey.UsesFmt {
			return true
		}
	}
	return false
}

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

// =============================================================================
// Template functions and code generation
// =============================================================================

var tmplFuncs = template.FuncMap{
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
	"pkParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	"pkArgs": func(kd keyData) string {
		var args []string
		for _, p := range kd.Params {
			args = append(args, p.Name)
		}
		return strings.Join(args, ", ")
	},
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
	"skEqualsFormatExpr": func(kd keyData) string { return kd.FormatExpr },
	"skBeginsWithExpr": func(kd keyData) string {
		if kd.LiteralPrefix != "" {
			return fmt.Sprintf("%q + prefix", kd.LiteralPrefix)
		}
		return "prefix"
	},
	"skBetweenStartExpr": func(kd keyData) string { return buildSKBoundExpr(kd, "Start") },
	"skBetweenEndExpr":   func(kd keyData) string { return buildSKBoundExpr(kd, "End") },
	"skBoundExpr":        func(kd keyData) string { return buildSKBoundExpr(kd, "") },
	"skBeginsWithParams": func(kd keyData) string { return "prefix string" },
	"skEqualsParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	"skBetweenParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%sStart %s, %sEnd %s", p.Name, p.Type, p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
	"skSingleValueParams": func(kd keyData) string {
		var parts []string
		for _, p := range kd.Params {
			parts = append(parts, fmt.Sprintf("%s %s", p.Name, p.Type))
		}
		return strings.Join(parts, ", ")
	},
}

// generateCode transforms index info into Go source code using the index template.
// GenerateCode transforms index info into Go source code using the index template.
func GenerateCode(packageName string, indexes []IndexInfo) ([]byte, error) {
	var idxDataList []indexData
	needsFmt, needsStrconv, needsTime := false, false, false

	for _, idx := range indexes {
		data, err := buildIndexData(idx)
		if err != nil {
			return nil, fmt.Errorf("building data for %s: %w", idx.VarName, err)
		}
		idxDataList = append(idxDataList, data)
		if needsFmtImport(data) {
			needsFmt = true
		}
		if needsStrconvImport(data) {
			needsStrconv = true
		}
		if needsTimeImport(data) {
			needsTime = true
		}
	}

	imports := []string{
		`"github.com/acksell/bezos/dynamodb/ddbsdk"`,
		`"github.com/acksell/bezos/dynamodb/index"`,
		`"github.com/acksell/bezos/dynamodb/index/indices"`,
		`"github.com/acksell/bezos/dynamodb/table"`,
	}
	if needsFmt {
		imports = append([]string{`"fmt"`}, imports...)
	}
	if needsStrconv {
		imports = append([]string{`"strconv"`}, imports...)
	}
	if needsTime {
		imports = append([]string{`"time"`}, imports...)
	}

	tmplData := struct {
		Package string
		Imports []string
		Indexes []indexData
	}{packageName, imports, idxDataList}

	tmpl, err := template.New("index.tmpl").Funcs(tmplFuncs).ParseFS(templates, "template/index.tmpl")
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, tmplData); err != nil {
		return nil, fmt.Errorf("executing template: %w", err)
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return buf.Bytes(), fmt.Errorf("formatting generated code: %w\n%s", err, buf.String())
	}
	return formatted, nil
}
