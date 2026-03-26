// Package ddbcli provides runtime key construction and schema utilities
// for the ddb CLI. It builds DynamoDB keys from schema patterns and
// user-provided field values without requiring code generation.
package ddbcli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/schema"
)

// Param describes a required parameter for constructing entity keys.
type Param struct {
	Name   string // field tag (e.g. "id", "tenantID")
	Type   string // Go type (e.g. "string", "int64", "time.Time")
	Source string // "partitionKey", "sortKey", "gsi:<name>:partitionKey", etc.
}

// EntityMatch holds a matched entity and its parent table.
type EntityMatch struct {
	Entity schema.Entity
	Table  schema.Table
}

// FindEntity looks up an entity by name (case-insensitive) across all schemas.
func FindEntity(schemas []schema.Schema, name string) (EntityMatch, bool) {
	for _, s := range schemas {
		for _, t := range s.Tables {
			for _, e := range t.Entities {
				if strings.EqualFold(e.Type, name) {
					return EntityMatch{Entity: e, Table: t}, true
				}
			}
		}
	}
	return EntityMatch{}, false
}

// RequiredParams extracts the parameters needed to construct the primary
// key (pk + sk) for an entity from its key patterns.
func RequiredParams(e schema.Entity) []Param {
	fieldTypes := fieldTypeMap(e)
	var params []Param
	params = append(params, paramsFromPattern(e.PartitionKeyPattern, fieldTypes, "partitionKey")...)
	params = append(params, paramsFromPattern(e.SortKeyPattern, fieldTypes, "sortKey")...)
	return params
}

// RequiredPKParams extracts only the partition key parameters.
func RequiredPKParams(e schema.Entity) []Param {
	return paramsFromPattern(e.PartitionKeyPattern, fieldTypeMap(e), "partitionKey")
}

// RequiredSKParams extracts only the sort key parameters.
func RequiredSKParams(e schema.Entity) []Param {
	return paramsFromPattern(e.SortKeyPattern, fieldTypeMap(e), "sortKey")
}

// GSIParams extracts the parameters needed for a GSI query.
func GSIParams(e schema.Entity, gsiName string) (pk []Param, sk []Param, err error) {
	fieldTypes := fieldTypeMap(e)
	for _, m := range e.GSIMappings {
		if strings.EqualFold(m.GSI, gsiName) {
			pk = paramsFromPattern(m.PartitionPattern, fieldTypes, "gsi:"+gsiName+":partitionKey")
			sk = paramsFromPattern(m.SortPattern, fieldTypes, "gsi:"+gsiName+":sortKey")
			return pk, sk, nil
		}
	}
	return nil, nil, fmt.Errorf("entity %q has no GSI mapping for %q", e.Type, gsiName)
}

// BuildKey substitutes field values into a key pattern string.
// values maps field tag names to their string representation.
// fieldTypes maps field tag names to Go type strings for coercion.
//
// For example:
//
//	BuildKey("USER#{id}", {"id": "string"}, {"id": "abc123"})
//	// returns "USER#abc123", nil
func BuildKey(pattern string, fieldTypes map[string]string, values map[string]string) (string, error) {
	if pattern == "" {
		return "", nil
	}

	spec, err := val.ParseFmt(pattern)
	if err != nil {
		return "", fmt.Errorf("parsing pattern %q: %w", pattern, err)
	}

	var b strings.Builder
	for _, part := range spec.Parts {
		if part.IsLiteral {
			b.WriteString(part.Value)
			continue
		}

		fieldName := part.Value
		raw, ok := values[fieldName]
		if !ok {
			return "", fmt.Errorf("missing required field %q", fieldName)
		}

		fieldType := fieldTypes[fieldName]
		if fieldType == "" {
			fieldType = "string"
		}

		formatted, err := formatValue(raw, fieldType, part)
		if err != nil {
			return "", fmt.Errorf("field %q: %w", fieldName, err)
		}
		b.WriteString(formatted)
	}

	return b.String(), nil
}

// BuildKeyFromEntity builds a key using an entity's pattern and field type info.
func BuildKeyFromEntity(pattern string, entity schema.Entity, values map[string]string) (string, error) {
	return BuildKey(pattern, fieldTypeMap(entity), values)
}

// LiteralPrefix extracts the literal prefix from a key pattern (before the first {field}).
// Returns the full pattern if it contains no field references.
func LiteralPrefix(pattern string) string {
	if pattern == "" {
		return ""
	}
	idx := strings.Index(pattern, "{")
	if idx < 0 {
		return pattern
	}
	return pattern[:idx]
}

// formatValue converts a raw string value to the format expected by the key pattern,
// applying type coercion and printf formatting as specified.
func formatValue(raw string, fieldType string, part val.SpecPart) (string, error) {
	switch {
	case fieldType == "string":
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, raw), nil
		}
		return raw, nil

	case isIntegerType(fieldType):
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return "", fmt.Errorf("expected integer for type %s, got %q", fieldType, raw)
		}
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, n), nil
		}
		return strconv.FormatInt(n, 10), nil

	case isUintType(fieldType):
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return "", fmt.Errorf("expected unsigned integer for type %s, got %q", fieldType, raw)
		}
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, n), nil
		}
		return strconv.FormatUint(n, 10), nil

	case isFloatType(fieldType):
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return "", fmt.Errorf("expected float for type %s, got %q", fieldType, raw)
		}
		spec := part.PrintfSpec
		if spec == "" {
			spec = part.Format()
		}
		if spec == "" {
			spec = "%g"
		}
		return fmt.Sprintf(spec, f), nil

	case fieldType == "time.Time":
		return formatTimeValue(raw, part)

	default:
		// Unknown type, treat as string
		return raw, nil
	}
}

// formatTimeValue handles time.Time field formatting.
// Accepts RFC3339, unix timestamp (seconds), or unix millis/nanos.
func formatTimeValue(raw string, part val.SpecPart) (string, error) {
	t, err := parseTimeInput(raw)
	if err != nil {
		return "", err
	}

	if part.HasModifier("utc") {
		t = t.UTC()
	}

	format := part.Format()
	switch format {
	case "unix":
		v := t.Unix()
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, v), nil
		}
		return strconv.FormatInt(v, 10), nil
	case "unixmilli":
		v := t.UnixMilli()
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, v), nil
		}
		return strconv.FormatInt(v, 10), nil
	case "unixnano":
		v := t.UnixNano()
		if part.PrintfSpec != "" {
			return fmt.Sprintf(part.PrintfSpec, v), nil
		}
		return strconv.FormatInt(v, 10), nil
	case "rfc3339":
		return t.Format(time.RFC3339), nil
	case "rfc3339fixed":
		return t.Format("2006-01-02T15:04:05.000000000Z07:00"), nil
	case "rfc3339nano":
		return t.Format(time.RFC3339Nano), nil
	default:
		if format != "" {
			return t.Format(format), nil
		}
		return t.Format(time.RFC3339), nil
	}
}

// parseTimeInput tries to parse a time string in various formats.
func parseTimeInput(raw string) (time.Time, error) {
	// Try RFC3339 first
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, nil
	}

	// Try as unix timestamp (seconds)
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		// Heuristic: if > 1e15, assume nanoseconds; if > 1e12, assume millis; else seconds
		switch {
		case n > 1e15:
			return time.Unix(0, n), nil
		case n > 1e12:
			return time.Unix(0, n*int64(time.Millisecond)), nil
		default:
			return time.Unix(n, 0), nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse time %q (expected RFC3339 or unix timestamp)", raw)
}

func paramsFromPattern(pattern string, fieldTypes map[string]string, source string) []Param {
	if pattern == "" {
		return nil
	}

	spec, err := val.ParseFmt(pattern)
	if err != nil {
		return nil
	}

	var params []Param
	for _, part := range spec.Parts {
		if part.IsLiteral {
			continue
		}
		typ := fieldTypes[part.Value]
		if typ == "" {
			typ = "string"
		}
		params = append(params, Param{
			Name:   part.Value,
			Type:   typ,
			Source: source,
		})
	}
	return params
}

func fieldTypeMap(e schema.Entity) map[string]string {
	m := make(map[string]string, len(e.Fields))
	for _, f := range e.Fields {
		m[f.Tag] = f.Type
	}
	return m
}

func isIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

func isUintType(t string) bool {
	switch t {
	case "uint", "uint8", "uint16", "uint32", "uint64":
		return true
	}
	return false
}

func isFloatType(t string) bool {
	return t == "float32" || t == "float64"
}
