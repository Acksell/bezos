package ddbgen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
)

// IndexInfo holds discovered information about a PrimaryIndex variable.
type IndexInfo struct {
	// VarName is the variable name (e.g., "userIndex")
	VarName string
	// EntityType is the Go type name of the entity (e.g., "User")
	EntityType string
	// PartitionKey is the partition key pattern (e.g., "USER#{id}")
	PartitionKey string
	// SortKey is the sort key pattern, empty if none
	SortKey string
	// GSIs holds information about secondary indexes
	GSIs []GSIInfo
}

// GSIInfo holds discovered information about a SecondaryIndex.
type GSIInfo struct {
	// Name is the GSI name
	Name string
	// Index is the position in the Secondary slice
	Index int
	// PKDef is the partition key definition name (e.g., "gsi1pk")
	PKDef string
	// PKPattern is the partition key pattern
	PKPattern string
	// SKDef is the sort key definition name, empty if none
	SKDef string
	// SKPattern is the sort key pattern, empty if none
	SKPattern string
}

// FieldInfo holds information about a struct field relevant to key generation.
type FieldInfo struct {
	// Name is the Go field name (e.g., "UserID")
	Name string
	// Tag is the dynamodbav tag value (e.g., "id")
	Tag string
	// Type is the Go type (e.g., "string")
	Type string
}

// DiscoverResult holds all discovered indexes and type information from a package.
type DiscoverResult struct {
	// PackageName is the Go package name
	PackageName string
	// PackagePath is the import path
	PackagePath string
	// Indexes are the discovered index definitions
	Indexes []IndexInfo
	// EntityFields maps entity type names to their fields
	EntityFields map[string][]FieldInfo
}

// Discover scans the specified directory for PrimaryIndex definitions.
func Discover(dir string) (*DiscoverResult, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedTypes |
			packages.NeedTypesInfo |
			packages.NeedSyntax |
			packages.NeedImports,
		Dir: dir,
	}

	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return nil, fmt.Errorf("loading package: %w", err)
	}

	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no packages found in %s", dir)
	}

	pkg := pkgs[0]
	if len(pkg.Errors) > 0 {
		var errs []string
		for _, e := range pkg.Errors {
			errs = append(errs, e.Error())
		}
		return nil, fmt.Errorf("package errors: %s", strings.Join(errs, "; "))
	}

	result := &DiscoverResult{
		PackageName:  pkg.Name,
		PackagePath:  pkg.PkgPath,
		EntityFields: make(map[string][]FieldInfo),
	}

	// Find all PrimaryIndex variable declarations
	for _, file := range pkg.Syntax {
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.VAR {
				continue
			}

			for _, spec := range genDecl.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok || len(valueSpec.Names) == 0 || len(valueSpec.Values) == 0 {
					continue
				}

				varName := valueSpec.Names[0].Name

				// Check if this is a PrimaryIndex type
				obj := pkg.TypesInfo.Defs[valueSpec.Names[0]]
				if obj == nil {
					continue
				}

				varType := obj.Type()
				if varType == nil {
					continue
				}

				// Check for PrimaryIndex (could be pointer or value)
				entityType := extractPrimaryIndexEntityType(varType)
				if entityType == "" {
					continue
				}

				// Parse the composite literal to extract key patterns
				indexInfo, err := parseIndexLiteral(varName, entityType, valueSpec.Values[0])
				if err != nil {
					return nil, fmt.Errorf("parsing index %s: %w", varName, err)
				}

				result.Indexes = append(result.Indexes, indexInfo)

				// Extract entity field information
				if _, exists := result.EntityFields[entityType]; !exists {
					fields := extractEntityFields(pkg.Types, entityType)
					result.EntityFields[entityType] = fields
				}
			}
		}
	}

	return result, nil
}

// extractPrimaryIndexEntityType returns the entity type name if t is a PrimaryIndex[E], empty string otherwise.
func extractPrimaryIndexEntityType(t types.Type) string {
	// Handle pointer types
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}

	// Check if it's a named type
	named, ok := t.(*types.Named)
	if !ok {
		return ""
	}

	// Check the type name
	obj := named.Obj()
	if obj.Name() != "PrimaryIndex" {
		return ""
	}

	// Check it's from our index package
	pkg := obj.Pkg()
	if pkg == nil || !strings.HasSuffix(pkg.Path(), "dynamodb/index") {
		return ""
	}

	// Extract the type argument
	typeArgs := named.TypeArgs()
	if typeArgs == nil || typeArgs.Len() != 1 {
		return ""
	}

	// Get the entity type name
	entityType := typeArgs.At(0)
	if named, ok := entityType.(*types.Named); ok {
		return named.Obj().Name()
	}

	return ""
}

// parseIndexLiteral extracts key patterns from a PrimaryIndex composite literal.
func parseIndexLiteral(varName, entityType string, expr ast.Expr) (IndexInfo, error) {
	info := IndexInfo{
		VarName:    varName,
		EntityType: entityType,
	}

	// Handle address-of operator (&index.PrimaryIndex{...})
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		expr = unary.X
	}

	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return info, fmt.Errorf("expected composite literal, got %T", expr)
	}

	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}

		switch key.Name {
		case "PartitionKey":
			if str := extractFmtPattern(kv.Value); str != "" {
				info.PartitionKey = str
			}
		case "SortKey":
			if str := extractFmtPattern(kv.Value); str != "" {
				info.SortKey = str
			}
		case "Secondary":
			gsis, err := parseSecondarySlice(kv.Value)
			if err != nil {
				return info, fmt.Errorf("parsing Secondary: %w", err)
			}
			info.GSIs = gsis
		}
	}

	if info.PartitionKey == "" {
		return info, fmt.Errorf("PartitionKey is required")
	}

	return info, nil
}

// parseSecondarySlice parses a []SecondaryIndex composite literal.
func parseSecondarySlice(expr ast.Expr) ([]GSIInfo, error) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("expected composite literal for Secondary slice")
	}

	var gsis []GSIInfo
	for i, elt := range lit.Elts {
		gsi, err := parseSecondaryIndex(i, elt)
		if err != nil {
			return nil, fmt.Errorf("GSI[%d]: %w", i, err)
		}
		gsis = append(gsis, gsi)
	}

	return gsis, nil
}

// parseSecondaryIndex parses a SecondaryIndex composite literal.
func parseSecondaryIndex(idx int, expr ast.Expr) (GSIInfo, error) {
	gsi := GSIInfo{Index: idx}

	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return gsi, fmt.Errorf("expected composite literal")
	}

	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}

		switch key.Name {
		case "Name":
			gsi.Name = extractStringLiteral(kv.Value)
		case "PartitionKey":
			def, pattern := parseKeyLiteral(kv.Value)
			gsi.PKDef = def
			gsi.PKPattern = pattern
		case "SortKey":
			// SortKey is *keys.Key, might be address-of
			def, pattern := parseKeyLiteral(kv.Value)
			gsi.SKDef = def
			gsi.SKPattern = pattern
		}
	}

	return gsi, nil
}

// parseKeyLiteral parses a keys.Key composite literal, returning (defName, pattern).
func parseKeyLiteral(expr ast.Expr) (string, string) {
	// Handle address-of operator
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		expr = unary.X
	}

	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return "", ""
	}

	var defName, pattern string

	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}

		switch key.Name {
		case "Def":
			defName = parseKeyDefName(kv.Value)
		case "Spec":
			pattern = extractFmtPattern(kv.Value)
		}
	}

	return defName, pattern
}

// parseKeyDefName extracts the Name field from a table.KeyDef composite literal.
func parseKeyDefName(expr ast.Expr) string {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return ""
	}

	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}

		if key.Name == "Name" {
			return extractStringLiteral(kv.Value)
		}
	}

	return ""
}

// extractStringLiteral extracts a string value from a basic literal.
func extractStringLiteral(expr ast.Expr) string {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}

	// Remove quotes
	s := lit.Value
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
		if s[0] == '`' && s[len(s)-1] == '`' {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// extractFmtPattern extracts a pattern string from keys.Fmt("...") or keys.Fmt("...").Ptr()
func extractFmtPattern(expr ast.Expr) string {
	// Handle keys.Fmt("...").Ptr() - a call expression where Fun is a selector
	if call, ok := expr.(*ast.CallExpr); ok {
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			// Check if this is a .Ptr() call
			if sel.Sel.Name == "Ptr" {
				// Recursively get pattern from the inner expression
				return extractFmtPattern(sel.X)
			}
			// Check if this is keys.Fmt("...") - selector on package name
			if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "keys" && sel.Sel.Name == "Fmt" {
				if len(call.Args) == 1 {
					return extractStringLiteral(call.Args[0])
				}
			}
		}
	}
	return ""
}

// extractEntityFields extracts field information from a struct type.
func extractEntityFields(pkg *types.Package, typeName string) []FieldInfo {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		return nil
	}

	named, ok := obj.Type().(*types.Named)
	if !ok {
		return nil
	}

	underlying := named.Underlying()
	structType, ok := underlying.(*types.Struct)
	if !ok {
		return nil
	}

	var fields []FieldInfo
	for i := 0; i < structType.NumFields(); i++ {
		field := structType.Field(i)
		if !field.Exported() {
			continue
		}

		tag := structType.Tag(i)
		tagValue := parseStructTag(tag, "dynamodbav")
		if tagValue == "" {
			tagValue = parseStructTag(tag, "json")
		}
		if tagValue == "" {
			tagValue = field.Name() // Default to field name
		}
		if tagValue == "-" {
			continue // Skip ignored fields
		}

		fields = append(fields, FieldInfo{
			Name: field.Name(),
			Tag:  tagValue,
			Type: typeString(field.Type()),
		})
	}

	return fields
}

// parseStructTag extracts a tag value from a struct tag string.
func parseStructTag(tag, key string) string {
	// Simple parser for `key:"value,options"`
	prefix := key + `:"`
	idx := strings.Index(tag, prefix)
	if idx == -1 {
		return ""
	}

	start := idx + len(prefix)
	end := strings.Index(tag[start:], `"`)
	if end == -1 {
		return ""
	}

	value := tag[start : start+end]

	// Strip options after comma
	if comma := strings.Index(value, ","); comma != -1 {
		value = value[:comma]
	}

	return value
}

// typeString returns a string representation of a type.
func typeString(t types.Type) string {
	switch t := t.(type) {
	case *types.Basic:
		return t.Name()
	case *types.Named:
		return t.Obj().Name()
	case *types.Pointer:
		return "*" + typeString(t.Elem())
	case *types.Slice:
		return "[]" + typeString(t.Elem())
	default:
		return t.String()
	}
}
