package ddbgen

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// IndexVar holds the minimal type-level information about a discovered PrimaryIndex variable.
// The actual index values (table name, key patterns, GSI info) are read at runtime by the sidecar.
type IndexVar struct {
	// VarName is the variable name (e.g., "userIndex")
	VarName string
	// EntityType is the Go type name of the entity (e.g., "User")
	EntityType string
	// IsVersioned is true if the entity implements VersionedDynamoEntity
	IsVersioned bool
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
	// Indexes are the discovered index variable definitions
	Indexes []IndexVar
	// EntityFields maps entity type names to their fields
	EntityFields map[string][]FieldInfo
}

// Discover scans the specified directory for PrimaryIndex definitions.
// It performs minimal type-checking to extract variable names, entity type args,
// versioned status, and entity struct fields. It does NOT parse composite literals
// or resolve key patterns — that is done at runtime by the sidecar program.
func Discover(dir string) (*DiscoverResult, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedTypes |
			packages.NeedTypesInfo |
			packages.NeedSyntax,
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

	// Walk the package scope to find PrimaryIndex variables
	scope := pkg.Types.Scope()
	type indexWithPos struct {
		idx IndexVar
		pos token.Position // resolved file:line position for stable ordering
	}
	var found []indexWithPos
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if obj == nil {
			continue
		}

		// Only look at variables
		v, ok := obj.(*types.Var)
		if !ok {
			continue
		}

		entityType := extractPrimaryIndexEntityType(v.Type())
		if entityType == "" {
			continue
		}

		idx := IndexVar{
			VarName:     name,
			EntityType:  entityType,
			IsVersioned: implementsVersionedEntity(pkg.Types, entityType),
		}
		found = append(found, indexWithPos{idx: idx, pos: pkg.Fset.Position(v.Pos())})

		// Extract entity field information
		if _, exists := result.EntityFields[entityType]; !exists {
			fields := extractEntityFields(pkg.Types, entityType)
			result.EntityFields[entityType] = fields
		}
	}

	// Sort by filename then line number for deterministic output order.
	// Using raw token.Pos is NOT stable when indexes span multiple files,
	// because go/packages may assign different file base offsets across runs.
	sort.Slice(found, func(i, j int) bool {
		fi, fj := filepath.Base(found[i].pos.Filename), filepath.Base(found[j].pos.Filename)
		if fi != fj {
			return fi < fj
		}
		return found[i].pos.Line < found[j].pos.Line
	})
	for _, f := range found {
		result.Indexes = append(result.Indexes, f.idx)
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
	prefix := key + `:"` //nolint:goconst
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
// This is done by checking if *EntityType has a VersionField() method.
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
