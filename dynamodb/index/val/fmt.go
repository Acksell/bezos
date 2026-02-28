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
//   - "{ts:utc:rfc3339}"             → time.Time converted to UTC then formatted
//   - "{ts:utc:unixnano:%020d}"      → chained modifiers: UTC + unixnano + padding
//
// Supported time formats: unix, unixmilli, unixnano, rfc3339, rfc3339fixed, rfc3339nano,
// or any custom Go time layout string.
//
// Supported modifiers:
//   - utc: Converts time.Time to UTC before formatting (recommended for sort keys)
type FmtSpec struct {
	Raw   string     // Original pattern string
	Kind  SpecKind   // DynamoDB attribute type (S, N, B)
	Parts []SpecPart // Parsed literal and field reference parts
}

// SpecPart represents a single part of a format pattern - either a literal string
// or a field reference with optional format annotations.
type SpecPart struct {
	IsLiteral  bool     // true if this is a literal string, false if field reference
	Value      string   // the literal value or field path (e.g., "user.id")
	Formats    []string // format modifiers in order (e.g., ["utc", "rfc3339"] or ["unixnano"])
	PrintfSpec string   // printf format spec (e.g., "%020d") - only for field refs
}

// Format returns the primary format (last non-printf modifier), for backwards compatibility.
// For "{ts:utc:rfc3339}" returns "rfc3339". For "{ts:utc:unixnano}" returns "unixnano".
func (p SpecPart) Format() string {
	if len(p.Formats) == 0 {
		return ""
	}
	return p.Formats[len(p.Formats)-1]
}

// HasModifier returns true if the given modifier is in the format chain.
// E.g., for "{ts:utc:rfc3339}", HasModifier("utc") returns true.
func (p SpecPart) HasModifier(mod string) bool {
	for _, f := range p.Formats {
		if f == mod {
			return true
		}
	}
	return false
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

// parseFieldRef parses a field reference string like "field", "field:format", "field:format:%spec",
// or "field:mod1:mod2:format" for chained modifiers.
//
// Examples:
//   - "ts" → fieldPath="ts", formats=[], printfSpec=""
//   - "ts:unix" → fieldPath="ts", formats=["unix"], printfSpec=""
//   - "ts:unixnano:%020d" → fieldPath="ts", formats=["unixnano"], printfSpec="%020d"
//   - "ts:utc:rfc3339" → fieldPath="ts", formats=["utc", "rfc3339"], printfSpec=""
//   - "ts:utc:unixnano:%020d" → fieldPath="ts", formats=["utc", "unixnano"], printfSpec="%020d"
func parseFieldRef(ref string) (fieldPath string, formats []string, printfSpec string) {
	parts := strings.Split(ref, ":")
	fieldPath = parts[0]

	if len(parts) == 1 {
		return
	}

	// Check if the last part is a printf spec (starts with %)
	// todo why only last part?
	lastPart := parts[len(parts)-1]
	if strings.HasPrefix(lastPart, "%") {
		printfSpec = lastPart
		formats = parts[1 : len(parts)-1]
	} else {
		formats = parts[1:]
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

		fieldPath, formats, printfSpec := parseFieldRef(fullRef)

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
			Formats:    formats,
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
