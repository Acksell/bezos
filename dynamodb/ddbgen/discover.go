package ddbgen

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
)

// KeyKind indicates the type of key value (string, number, or bytes).
type KeyKind string

const (
	KeyKindString KeyKind = "S" // String (val.Fmt, val.String)
	KeyKindNumber KeyKind = "N" // Number (val.Number)
	KeyKindBytes  KeyKind = "B" // Binary (val.Bytes - base64 encoded)
)

// KeyPattern holds a key pattern and its kind.
type KeyPattern struct {
	Pattern string
	Kind    KeyKind
}

// IndexInfo holds discovered information about a PrimaryIndex variable.
type IndexInfo struct {
	// VarName is the variable name (e.g., "userIndex")
	VarName string
	// EntityType is the Go type name of the entity (e.g., "User")
	EntityType string
	// TableName is the DynamoDB table name (e.g., "users")
	TableName string
	// PKDefName is the partition key attribute name (e.g., "pk")
	PKDefName string
	// SKDefName is the sort key attribute name (e.g., "sk")
	SKDefName string
	// PartitionKey is the partition key pattern (e.g., "USER#{id}")
	PartitionKey KeyPattern
	// SortKey is the sort key pattern, empty if none
	SortKey KeyPattern
	// GSIs holds information about secondary indexes
	GSIs []GSIInfo
	// IsVersioned is true if the entity implements VersionedDynamoEntity
	IsVersioned bool
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
	PKPattern KeyPattern
	// SKDef is the sort key definition name, empty if none
	SKDef string
	// SKPattern is the sort key pattern, empty if none
	SKPattern KeyPattern
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
				indexInfo, err := parseIndexLiteral(varName, entityType, valueSpec.Values[0], pkg.Syntax)
				if err != nil {
					return nil, fmt.Errorf("parsing index %s: %w", varName, err)
				}

				// Check if entity implements VersionedDynamoEntity
				indexInfo.IsVersioned = implementsVersionedEntity(pkg.Types, entityType)

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
func parseIndexLiteral(varName, entityType string, expr ast.Expr, files []*ast.File) (IndexInfo, error) {
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

	var tableExpr ast.Expr
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
		case "Table":
			tableExpr = kv.Value
			// Resolve table definition to get name and key def names
			tableDef := resolveTableDefinition(tableExpr, files)
			info.TableName = tableDef.Name
			info.PKDefName = tableDef.PKDefName
			info.SKDefName = tableDef.SKDefName
		case "PartitionKey":
			if kp := extractKeyPattern(kv.Value); kp.Pattern != "" {
				info.PartitionKey = kp
			}
		case "SortKey":
			if kp := extractKeyPattern(kv.Value); kp.Pattern != "" {
				info.SortKey = kp
			}
		case "Secondary":
			gsis, err := parseSecondarySlice(kv.Value, tableExpr, files)
			if err != nil {
				return info, fmt.Errorf("parsing Secondary: %w", err)
			}
			info.GSIs = gsis
		}
	}

	if info.PartitionKey.Pattern == "" {
		return info, fmt.Errorf("PartitionKey is required")
	}

	return info, nil
}

// resolvedTableDef holds resolved table definition information.
type resolvedTableDef struct {
	Name      string
	PKDefName string
	SKDefName string
}

// resolveTableDefinition extracts table name and key definition names from a table variable.
func resolveTableDefinition(tableExpr ast.Expr, files []*ast.File) resolvedTableDef {
	var def resolvedTableDef
	if tableExpr == nil || files == nil {
		return def
	}

	// tableExpr should be an identifier like "UserTable"
	ident, ok := tableExpr.(*ast.Ident)
	if !ok {
		return def
	}
	tableName := ident.Name

	// Find the table variable declaration in the package
	for _, file := range files {
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.VAR {
				continue
			}
			for _, spec := range genDecl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) == 0 || vs.Names[0].Name != tableName {
					continue
				}
				if len(vs.Values) == 0 {
					continue
				}
				return extractTableDefFromLiteral(vs.Values[0])
			}
		}
	}
	return def
}

// extractTableDefFromLiteral parses a TableDefinition composite literal.
func extractTableDefFromLiteral(expr ast.Expr) resolvedTableDef {
	var def resolvedTableDef
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return def
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
			def.Name = extractStringLiteral(kv.Value)
		case "KeyDefinitions":
			def.PKDefName, def.SKDefName = parsePrimaryKeyDefFields(kv.Value)
		}
	}
	return def
}

// parseSecondarySlice parses a []SecondaryIndex composite literal.
func parseSecondarySlice(expr ast.Expr, tableExpr ast.Expr, files []*ast.File) ([]GSIInfo, error) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("expected composite literal for Secondary slice")
	}

	// Pre-resolve all GSI definitions from the table variable
	gsiDefs := resolveTableGSIDefs(tableExpr, files)

	var gsis []GSIInfo
	for i, elt := range lit.Elts {
		gsi, err := parseSecondaryIndex(i, elt, gsiDefs)
		if err != nil {
			return nil, fmt.Errorf("GSI[%d]: %w", i, err)
		}
		gsis = append(gsis, gsi)
	}

	return gsis, nil
}

// parseSecondaryIndex parses a SecondaryIndex composite literal.
// The new SecondaryIndex struct has: GSI table.GSIDefinition, Partition val.ValDef, Sort *val.ValDef
func parseSecondaryIndex(idx int, expr ast.Expr, gsiDefs []resolvedGSIDef) (GSIInfo, error) {
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
		case "GSI":
			// Resolve the GSI definition.
			// Handles: TableVar.GSIs[N] or table.GSIDefinition{Name: "...", ...}
			name, pkDef, skDef := resolveGSIField(kv.Value, gsiDefs)
			gsi.Name = name
			gsi.PKDef = pkDef
			gsi.SKDef = skDef
		case "Partition":
			// Now a val.ValDef (val.Fmt("...")) not KeyValDef
			gsi.PKPattern = extractKeyPattern(kv.Value)
		case "Sort":
			// Now a *val.ValDef (val.Fmt("...").Ptr()) not *KeyValDef
			gsi.SKPattern = extractKeyPattern(kv.Value)
		}
	}

	return gsi, nil
}

// resolvedGSIDef holds a pre-resolved GSI definition from a table variable.
type resolvedGSIDef struct {
	Name  string
	PKDef string
	SKDef string
}

// resolveTableGSIDefs extracts GSI definitions from a table variable's composite literal.
func resolveTableGSIDefs(tableExpr ast.Expr, files []*ast.File) []resolvedGSIDef {
	if tableExpr == nil || files == nil {
		return nil
	}

	// tableExpr should be an identifier like "UserTable"
	ident, ok := tableExpr.(*ast.Ident)
	if !ok {
		return nil
	}
	tableName := ident.Name

	// Find the table variable declaration in the package
	for _, file := range files {
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.VAR {
				continue
			}
			for _, spec := range genDecl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) == 0 || vs.Names[0].Name != tableName {
					continue
				}
				if len(vs.Values) == 0 {
					continue
				}
				return extractGSIDefsFromTableLiteral(vs.Values[0])
			}
		}
	}
	return nil
}

// extractGSIDefsFromTableLiteral parses a TableDefinition composite literal's GSIs field.
func extractGSIDefsFromTableLiteral(expr ast.Expr) []resolvedGSIDef {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil
	}

	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "GSIs" {
			continue
		}
		// Parse the []GSIDefinition slice
		sliceLit, ok := kv.Value.(*ast.CompositeLit)
		if !ok {
			return nil
		}
		var defs []resolvedGSIDef
		for _, gsiElt := range sliceLit.Elts {
			def := parseGSIDefinitionLiteral(gsiElt)
			defs = append(defs, def)
		}
		return defs
	}
	return nil
}

// parseGSIDefinitionLiteral parses a table.GSIDefinition composite literal.
func parseGSIDefinitionLiteral(expr ast.Expr) resolvedGSIDef {
	var def resolvedGSIDef
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return def
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
			def.Name = extractStringLiteral(kv.Value)
		case "KeyDefinitions":
			def.PKDef, def.SKDef = parsePrimaryKeyDefFields(kv.Value)
		}
	}
	return def
}

// parsePrimaryKeyDefFields extracts PartitionKey.Name and SortKey.Name from a PrimaryKeyDefinition literal.
func parsePrimaryKeyDefFields(expr ast.Expr) (pkName, skName string) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return
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
			pkName = parseKeyDefName(kv.Value)
		case "SortKey":
			skName = parseKeyDefName(kv.Value)
		}
	}
	return
}

// resolveGSIField resolves a GSI field expression to its name and key def names.
// Handles: TableVar.GSIs[N] (resolved via gsiDefs) or table.GSIDefinition{...} literal.
func resolveGSIField(expr ast.Expr, gsiDefs []resolvedGSIDef) (name, pkDef, skDef string) {
	// Case 1: Inline composite literal table.GSIDefinition{Name: "...", ...}
	if lit, ok := expr.(*ast.CompositeLit); ok {
		def := parseGSIDefinitionLiteral(lit)
		return def.Name, def.PKDef, def.SKDef
	}

	// Case 2: TableVar.GSIs[N] — an index expression
	indexExpr, ok := expr.(*ast.IndexExpr)
	if !ok {
		return
	}

	// Extract the index N
	indexLit, ok := indexExpr.Index.(*ast.BasicLit)
	if !ok || indexLit.Kind != token.INT {
		return
	}
	idx := 0
	fmt.Sscanf(indexLit.Value, "%d", &idx)

	if idx >= 0 && idx < len(gsiDefs) {
		d := gsiDefs[idx]
		return d.Name, d.PKDef, d.SKDef
	}
	return
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

// extractLiteral extracts the string representation of a basic literal (string, int, or float).
func extractLiteral(expr ast.Expr) string {
	// Handle basic literals (string, int, float)
	if lit, ok := expr.(*ast.BasicLit); ok {
		switch lit.Kind {
		case token.STRING:
			return extractStringLiteral(expr)
		case token.INT, token.FLOAT:
			return lit.Value
		}
	}
	return ""
}

// extractKeyPattern extracts a key pattern from val.Fmt("..."), val.String("..."), val.Number(...), val.Bytes("..."), or .Ptr() variants
// TODO: Use the existing val package's AST logic instead of parsing text here, to ensure consistency and support all val features.
func extractKeyPattern(expr ast.Expr) KeyPattern {
	// Handle .Ptr() - a call expression where Fun is a selector
	if call, ok := expr.(*ast.CallExpr); ok {
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			// Check if this is a .Ptr() call
			if sel.Sel.Name == "Ptr" {
				// Recursively get pattern from the inner expression
				return extractKeyPattern(sel.X)
			}
			// Check if this is val.Fmt/String/Number/Bytes("...") - selector on package name
			if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "val" {
				if len(call.Args) == 1 {
					switch sel.Sel.Name {
					case "Fmt", "String":
						return KeyPattern{Pattern: extractLiteral(call.Args[0]), Kind: KeyKindString}
					case "Number":
						return KeyPattern{Pattern: extractLiteral(call.Args[0]), Kind: KeyKindNumber}
					case "Bytes":
						return KeyPattern{Pattern: extractLiteral(call.Args[0]), Kind: KeyKindBytes}
					}
				}
			}
		}
	}
	return KeyPattern{}
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

// implementsVersionedEntity checks if the entity type implements VersionedDynamoEntity.
// This is done by checking if *EntityType has a Version() method.
func implementsVersionedEntity(pkg *types.Package, typeName string) bool {
	obj := pkg.Scope().Lookup(typeName)
	if obj == nil {
		return false
	}

	named, ok := obj.Type().(*types.Named)
	if !ok {
		return false
	}

	// Check methods on pointer receiver (*Entity)
	ptrType := types.NewPointer(named)
	methodSet := types.NewMethodSet(ptrType)

	for i := 0; i < methodSet.Len(); i++ {
		method := methodSet.At(i)
		if method.Obj().Name() == "VersionField" {
			// Check signature: VersionField() (string, any)
			sig, ok := method.Obj().Type().(*types.Signature)
			if !ok {
				continue
			}
			// Should have no params and 2 results
			if sig.Params().Len() != 0 || sig.Results().Len() != 2 {
				continue
			}
			// First result should be string
			if basic, ok := sig.Results().At(0).Type().(*types.Basic); ok {
				if basic.Kind() == types.String {
					return true
				}
			}
		}
	}

	return false
}
