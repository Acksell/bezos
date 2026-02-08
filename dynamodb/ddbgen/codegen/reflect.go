package codegen

import (
	"fmt"
	"reflect"
	"strings"
)

// FieldMapping maps a DynamoDB attribute tag to Go field access info.
type FieldMapping struct {
	FieldName string // Go struct field name (e.g., "UserID")
	GoExpr    string // Go expression for field access (e.g., "e.UserID")
	FieldType string // Go type name (e.g., "string", "int")
}

// BuildTagToFieldMap creates a mapping from dynamodbav/json tag values to Go field names.
// It inspects the struct's fields and builds a map keyed by the tag value.
// For nested structs, it creates composite keys like "address.city".
func BuildTagToFieldMap(entityType any) (map[string]FieldMapping, error) {
	result := make(map[string]FieldMapping)
	t := reflect.TypeOf(entityType)

	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("entity must be a struct, got %s", t.Kind())
	}

	buildTagMapRecursive(t, nil, "e", result)
	return result, nil
}

func buildTagMapRecursive(t reflect.Type, tagPath []string, goPrefix string, result map[string]FieldMapping) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get the tag value - prefer dynamodbav, fall back to json, then field name
		tagValue := getTagName(field)
		if tagValue == "-" {
			continue // Skip fields marked to ignore
		}

		currentTagPath := append(append([]string{}, tagPath...), tagValue)
		goExpr := goPrefix + "." + field.Name

		// Build the full tag path key (e.g., "address.city" for nested structs)
		tagKey := strings.Join(currentTagPath, ".")

		// Store the mapping
		result[tagKey] = FieldMapping{
			FieldName: field.Name,
			GoExpr:    goExpr,
			FieldType: typeToString(field.Type),
		}

		// Also store with just the tag value for simple lookups
		// (only if it doesn't conflict with an existing entry)
		if _, exists := result[tagValue]; !exists {
			result[tagValue] = FieldMapping{
				FieldName: field.Name,
				GoExpr:    goExpr,
				FieldType: typeToString(field.Type),
			}
		}

		// Recurse into nested structs (but not pointers or special types)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Struct && !isSpecialType(fieldType) {
			buildTagMapRecursive(fieldType, currentTagPath, goExpr, result)
		}
	}
}

// getTagName extracts the field name from dynamodbav or json tag.
func getTagName(field reflect.StructField) string {
	// Try dynamodbav first (AWS SDK v2 tag)
	if tag, ok := field.Tag.Lookup("dynamodbav"); ok {
		return parseTagName(tag)
	}
	// Fall back to json tag
	if tag, ok := field.Tag.Lookup("json"); ok {
		return parseTagName(tag)
	}
	// No tag - use field name as-is (attributevalue.Marshal default behavior)
	return field.Name
}

// parseTagName extracts the name part from a tag value like "name,omitempty".
func parseTagName(tag string) string {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx]
	}
	return tag
}

// typeToString converts a reflect.Type to a string representation.
func typeToString(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return "*" + typeToString(t.Elem())
	case reflect.Slice:
		return "[]" + typeToString(t.Elem())
	case reflect.Map:
		return "map[" + typeToString(t.Key()) + "]" + typeToString(t.Elem())
	default:
		// For named types, include the package path if not builtin
		if t.PkgPath() != "" {
			return t.String() // Returns "package.Type"
		}
		return t.Name()
	}
}

// isSpecialType returns true for types we shouldn't recurse into.
func isSpecialType(t reflect.Type) bool {
	name := t.String()
	return name == "time.Time" ||
		strings.HasPrefix(name, "sql.Null") ||
		strings.HasPrefix(name, "types.") // DynamoDB types
}
