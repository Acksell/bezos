package val

import (
	"fmt"
	"regexp"
	"strings"
)

// SpecKind indicates the DynamoDB attribute type for a key spec.
type SpecKind string

const (
	SpecKindS SpecKind = "S" // String
	SpecKindN SpecKind = "N" // Number
	SpecKindB SpecKind = "B" // Binary
)

// FmtSpec represents a key format specification for introspection purposes.
// Patterns use {field} syntax for field references with optional format annotations:
//   - "PROFILE"                      → constant string
//   - "{count}"                      → single field reference
//   - "USER#{id}"                    → composite with field
//   - "ORDER#{a}#{b}"                → multiple field references
//   - "{user.id}"                    → nested field reference (dot notation)
//   - "{timestamp:unix}"             → time.Time with Unix seconds format
//   - "{timestamp:unixnano:%020d}"   → time.Time with padding
//   - "{count:%08d}"                 → integer with printf padding
//
// Supported time formats: unix, unixmilli, unixnano, rfc3339, rfc3339fixed, or custom Go layout.
type FmtSpec struct {
	Raw   string     // Original pattern string
	Kind  SpecKind   // DynamoDB attribute type (S, N, B)
	Parts []SpecPart // Parsed literal and field reference parts
}

// SpecPart represents a single part of a format pattern - either a literal string
// or a field reference with optional format annotations.
type SpecPart struct {
	IsLiteral  bool   // true if this is a literal string, false if field reference
	Value      string // the literal value or field path (e.g., "user.id")
	Format     string // format annotation (e.g., "unix", "rfc3339") - only for field refs
	PrintfSpec string // printf format spec (e.g., "%020d") - only for field refs
}

// fieldRefRegex matches {fieldName} or {nested.field.path} patterns (including empty braces for validation)
var fieldRefRegex = regexp.MustCompile(`\{([^}]*)\}`)

// Fmt creates a string key ValDef from a format pattern. Panics if the pattern is invalid.
//
// Examples:
//
//	val.Fmt("USER#{id}")              // composite string
//	val.Fmt("PROFILE")                // constant
//	val.Fmt("{createdAt}")            // single field reference
//	val.Fmt("{timestamp:unix}")       // time.Time with unix format
//	val.Fmt("{seq:%08d}")             // integer with padding
//	val.Fmt("{ts:unixnano:%020d}")    // time with format and padding
func Fmt(pattern string) ValDef {
	s, err := parseFmtSpec(pattern, SpecKindS)
	if err != nil {
		panic(fmt.Sprintf("val.Fmt: %v", err))
	}
	return ValDef{Format: &s}
}

// ParseFmt parses a format pattern string and returns the FmtSpec.
// Unlike Fmt, this returns an error instead of panicking.
func ParseFmt(pattern string) (*FmtSpec, error) {
	s, err := parseFmtSpec(pattern, SpecKindS)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// FromField creates a ValDef that copies directly from a field on the entity.
// Supports dot notation for nested fields (e.g., "user.id").
func FromField(fieldPath string) ValDef {
	return ValDef{FromField: fieldPath}
}

// parseFieldRef parses a field reference string like "field", "field:format", or "field:format:%spec"
// todo does not support e.g:
// .  val.Fmt("event#{ts:unixnxano:%020d}")
func parseFieldRef(ref string) (fieldPath, format, printfSpec string) {
	parts := strings.SplitN(ref, ":", 2)
	fieldPath = parts[0]

	if len(parts) > 1 {
		formatPart := parts[1]
		// Check if there's a printf spec (starts with %)
		if idx := strings.Index(formatPart, ":%"); idx >= 0 {
			format = formatPart[:idx]
			printfSpec = formatPart[idx+1:]
		} else if strings.HasPrefix(formatPart, "%") {
			// Just a printf spec, no named format
			printfSpec = formatPart
		} else {
			format = formatPart
		}
	}
	return
}

// parseFmtSpec parses a pattern string into a FmtSpec.
func parseFmtSpec(raw string, kind SpecKind) (FmtSpec, error) {
	if raw == "" {
		return FmtSpec{}, fmt.Errorf("pattern cannot be empty")
	}

	s := FmtSpec{Raw: raw, Kind: kind}

	// Find all field references
	matches := fieldRefRegex.FindAllStringSubmatchIndex(raw, -1)

	if len(matches) == 0 {
		// No field references - constant pattern
		s.Parts = []SpecPart{{IsLiteral: true, Value: raw}}
		return s, nil
	}

	lastEnd := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		fieldStart, fieldEnd := match[2], match[3]

		// Add literal part before this field reference (if any)
		if start > lastEnd {
			s.Parts = append(s.Parts, SpecPart{
				IsLiteral: true,
				Value:     raw[lastEnd:start],
			})
		}

		// Add field reference
		fullRef := raw[fieldStart:fieldEnd]
		if fullRef == "" {
			return FmtSpec{}, fmt.Errorf("empty field reference at position %d", start)
		}

		fieldPath, format, printfSpec := parseFieldRef(fullRef)

		// Validate field path components
		pathParts := strings.Split(fieldPath, ".")
		for i, part := range pathParts {
			if part == "" {
				return FmtSpec{}, fmt.Errorf("invalid field path %q: empty component at position %d", fieldPath, i)
			}
		}

		s.Parts = append(s.Parts, SpecPart{
			IsLiteral:  false,
			Value:      fieldPath,
			Format:     format,
			PrintfSpec: printfSpec,
		})

		lastEnd = end
	}

	// Add trailing literal (if any)
	if lastEnd < len(raw) {
		s.Parts = append(s.Parts, SpecPart{
			IsLiteral: true,
			Value:     raw[lastEnd:],
		})
	}

	return s, nil
}

// String returns the raw pattern string.
func (s FmtSpec) String() string {
	return s.Raw
}

// IsConstant returns true if this spec has no field references.
func (s FmtSpec) IsConstant() bool {
	return len(s.Parts) == 1 && s.Parts[0].IsLiteral
}

// IsZero returns true if this is a zero-value (uninitialized) FmtSpec.
func (s FmtSpec) IsZero() bool {
	return s.Raw == ""
}

// FieldRefs returns all field reference parts in the pattern, in order.
// For "ORDER#{tenant}#{id}", returns the SpecParts for tenant and id.
func (s FmtSpec) FieldRefs() []SpecPart {
	var refs []SpecPart
	for _, part := range s.Parts {
		if !part.IsLiteral {
			refs = append(refs, part)
		}
	}
	return refs
}

// FieldPaths returns all field paths in the pattern, in order.
// For "ORDER#{tenant}#{id}", returns ["tenant", "id"].
// For nested fields like "{user.id}", returns ["user.id"].
func (s FmtSpec) FieldPaths() []string {
	var paths []string
	for _, part := range s.Parts {
		if !part.IsLiteral {
			paths = append(paths, part.Value)
		}
	}
	return paths
}

// LiteralPrefix returns the leading literal portion before the first field reference.
// For "ORDER#{id}", returns "ORDER#". For "{id}", returns "".
func (s FmtSpec) LiteralPrefix() string {
	if len(s.Parts) > 0 && s.Parts[0].IsLiteral {
		return s.Parts[0].Value
	}
	return ""
}

// FieldPath returns the split field path for a SpecPart.
// For "user.id", returns ["user", "id"].
func (p SpecPart) FieldPath() []string {
	return strings.Split(p.Value, ".")
}

// ParamName returns a suitable function parameter name for this field ref.
// Uses the last component of the path (e.g., "user.id" → "id").
func (p SpecPart) ParamName() string {
	parts := p.FieldPath()
	return parts[len(parts)-1]
}

// =============================================================================
// Type checking helpers
// =============================================================================

// IsIntegerType returns true if the type is an integer type.
func IsIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64":
		return true
	}
	return false
}

// IsSignedIntegerType returns true if the type is a signed integer.
func IsSignedIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

// IsFloatType returns true if the type is a float type.
func IsFloatType(t string) bool {
	switch t {
	case "float32", "float64":
		return true
	}
	return false
}

// IsTimeType returns true if the type is time.Time.
func IsTimeType(t string) bool {
	return t == "time.Time" || t == "Time"
}

// HasPadding checks if a printf spec has leading zero padding (e.g., %020d).
func HasPadding(spec string) bool {
	if spec == "" {
		return false
	}
	// Check for %0Nd pattern
	return strings.HasPrefix(spec, "%0") && len(spec) > 2
}

// =============================================================================
// Code generation helpers
// =============================================================================

// ConversionResult holds the result of generating a type conversion expression.
type ConversionResult struct {
	Expr        string // Go expression that produces a string
	UsesStrconv bool   // True if strconv package is needed
	UsesTime    bool   // True if time package is needed
}

// TimeFormatLayouts maps format names to Go time layouts.
var TimeFormatLayouts = map[string]string{
	"rfc3339":      "time.RFC3339",
	"rfc3339fixed": `"2006-01-02T15:04:05.000000000Z07:00"`,
	"rfc3339nano":  "time.RFC3339Nano",
}

// GenerateConversionExpr generates a Go expression to convert a field value to string.
// fieldExpr is the Go expression for the field (e.g., "paramName" or "e.FieldName").
// fieldType is the Go type of the field (e.g., "int64", "time.Time").
// Returns error if the type/format combination is invalid.
func (p SpecPart) GenerateConversionExpr(fieldExpr string, fieldType string) (ConversionResult, error) {
	// String type - use directly (with optional format annotation for padding)
	if fieldType == "string" {
		if p.PrintfSpec != "" {
			return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", p.PrintfSpec, fieldExpr)}, nil
		}
		return ConversionResult{Expr: fieldExpr}, nil
	}

	// Integer types
	if IsIntegerType(fieldType) {
		if p.PrintfSpec != "" {
			return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", p.PrintfSpec, fieldExpr)}, nil
		}

		// Use strconv for unpadded integers
		if IsSignedIntegerType(fieldType) {
			if fieldType == "int64" {
				return ConversionResult{
					Expr:        fmt.Sprintf("strconv.FormatInt(%s, 10)", fieldExpr),
					UsesStrconv: true,
				}, nil
			}
			return ConversionResult{
				Expr:        fmt.Sprintf("strconv.FormatInt(int64(%s), 10)", fieldExpr),
				UsesStrconv: true,
			}, nil
		}
		// Unsigned
		if fieldType == "uint64" {
			return ConversionResult{
				Expr:        fmt.Sprintf("strconv.FormatUint(%s, 10)", fieldExpr),
				UsesStrconv: true,
			}, nil
		}
		return ConversionResult{
			Expr:        fmt.Sprintf("strconv.FormatUint(uint64(%s), 10)", fieldExpr),
			UsesStrconv: true,
		}, nil
	}

	// Float types - require explicit format
	if IsFloatType(fieldType) {
		if p.PrintfSpec == "" && p.Format == "" {
			return ConversionResult{}, fmt.Errorf("float type %s requires explicit format (e.g., {field:%%.2f} or {field:%%020.2f})", fieldType)
		}

		spec := p.PrintfSpec
		if spec == "" {
			spec = p.Format // Allow format to be used as spec for floats
		}

		return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s)", spec, fieldExpr)}, nil
	}

	// time.Time - require explicit format
	if IsTimeType(fieldType) {
		if p.Format == "" {
			return ConversionResult{}, fmt.Errorf("time.Time field requires explicit format (e.g., {field:unix}, {field:unixmilli}, {field:unixnano}, {field:rfc3339}, {field:rfc3339fixed}, or {field:2006-01-02})")
		}

		switch p.Format {
		case "unix":
			if p.PrintfSpec != "" {
				return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.Unix())", p.PrintfSpec, fieldExpr)}, nil
			}
			return ConversionResult{
				Expr:        fmt.Sprintf("strconv.FormatInt(%s.Unix(), 10)", fieldExpr),
				UsesStrconv: true,
			}, nil

		case "unixmilli":
			if p.PrintfSpec != "" {
				return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.UnixMilli())", p.PrintfSpec, fieldExpr)}, nil
			}
			return ConversionResult{
				Expr:        fmt.Sprintf("strconv.FormatInt(%s.UnixMilli(), 10)", fieldExpr),
				UsesStrconv: true,
			}, nil

		case "unixnano":
			if p.PrintfSpec != "" {
				return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(%q, %s.UnixNano())", p.PrintfSpec, fieldExpr)}, nil
			}
			return ConversionResult{
				Expr:        fmt.Sprintf("strconv.FormatInt(%s.UnixNano(), 10)", fieldExpr),
				UsesStrconv: true,
			}, nil

		case "rfc3339":
			return ConversionResult{
				Expr:     fmt.Sprintf("%s.Format(time.RFC3339)", fieldExpr),
				UsesTime: true,
			}, nil

		case "rfc3339fixed":
			return ConversionResult{
				Expr: fmt.Sprintf("%s.Format(%s)", fieldExpr, TimeFormatLayouts["rfc3339fixed"]),
			}, nil

		case "rfc3339nano":
			return ConversionResult{
				Expr:     fmt.Sprintf("%s.Format(time.RFC3339Nano)", fieldExpr),
				UsesTime: true,
			}, nil

		default:
			// Custom time format layout
			return ConversionResult{
				Expr: fmt.Sprintf("%s.Format(%q)", fieldExpr, p.Format),
			}, nil
		}
	}

	// Fall back to %v for unknown types
	return ConversionResult{Expr: fmt.Sprintf("fmt.Sprintf(\"%%v\", %s)", fieldExpr)}, nil
}

// SortKeyWarning returns a warning message if this field ref has sortability issues
// when used in a sort key. Returns empty string if no warning.
func (p SpecPart) SortKeyWarning(fieldType string, entityType string) string {
	if IsIntegerType(fieldType) && !HasPadding(p.PrintfSpec) {
		return fmt.Sprintf("warning: %s sort key uses %s without padding format.\n"+
			"  String comparison treats \"9\" > \"10\". For correct ordering either:\n"+
			"  - Use DynamoDB Number type for the sort key\n"+
			"  - Add padding: {field:%%020d}\n", entityType, fieldType)
	}

	if IsFloatType(fieldType) && !HasPadding(p.PrintfSpec) {
		spec := p.PrintfSpec
		if spec == "" {
			spec = p.Format
		}
		return fmt.Sprintf("warning: %s sort key uses %s format %q without total width padding.\n"+
			"  For correct string sorting, specify total width: {field:%%020.2f}\n", entityType, fieldType, spec)
	}

	if IsTimeType(fieldType) {
		switch p.Format {
		case "unix":
			if !HasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unix\" timestamp without padding.\n"+
					"  Unix timestamps change digit count (9 digits before 2001-09-09, 10 after).\n"+
					"  For correct string sorting, add padding: {field:unix:%%011d}\n", entityType)
			}
		case "unixmilli":
			if !HasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unixmilli\" timestamp without padding.\n"+
					"  Unix millisecond timestamps change digit count (12 digits before 2001-09-09, 13 after).\n"+
					"  For correct string sorting, add padding: {field:unixmilli:%%014d}\n", entityType)
			}
		case "unixnano":
			if !HasPadding(p.PrintfSpec) {
				return fmt.Sprintf("warning: %s sort key uses \"unixnano\" timestamp without padding.\n"+
					"  Unix nanosecond timestamps change digit count (18 digits before 2001-09-09, 19 after).\n"+
					"  For correct string sorting, add padding: {field:unixnano:%%020d}\n", entityType)
			}
		case "rfc3339":
			return fmt.Sprintf("warning: %s sort key uses \"rfc3339\" time format.\n"+
				"  RFC3339 has variable length and timezone-dependent sorting.\n"+
				"  For correct string ordering, prefer:\n"+
				"  - unix, unixmilli, or unixnano with padding (numeric, always sortable)\n"+
				"  - rfc3339fixed (constant length: \"2006-01-02T15:04:05.000000000Z07:00\")\n", entityType)
		case "rfc3339nano":
			return fmt.Sprintf("warning: %s sort key uses \"rfc3339nano\" time format.\n"+
				"  RFC3339Nano has variable length due to trailing zeros.\n"+
				"  For correct string ordering, prefer:\n"+
				"  - unix, unixmilli, or unixnano with padding\n"+
				"  - rfc3339fixed (constant length)\n", entityType)
		}
	}

	return ""
}

// GoParamType returns the Go type that a function parameter should have for this field ref.
// Normalizes types like "Time" to "time.Time".
func (p SpecPart) GoParamType(fieldType string) string {
	if fieldType == "string" {
		return "string"
	}
	if IsTimeType(fieldType) {
		return "time.Time"
	}
	return fieldType
}
