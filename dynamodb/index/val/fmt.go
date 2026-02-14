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
// Patterns use {field} syntax for field references:
//   - "PROFILE"           → constant string
//   - "{count}"           → single field reference
//   - "USER#{id}"         → composite with field
//   - "ORDER#{a}#{b}"     → multiple field references
//   - "{user.id}"         → nested field reference (dot notation)
type FmtSpec struct {
	raw   string
	kind  SpecKind
	parts []specPart
}

type specPart struct {
	literal bool   // true if this is a literal string, false if field reference
	value   string // the literal value or field reference (e.g., "user.id")
}

// fieldRefRegex matches {fieldName} or {nested.field.path} patterns (including empty braces for validation)
var fieldRefRegex = regexp.MustCompile(`\{([^}]*)\}`)

// Fmt creates a string key ValDef from a format pattern. Panics if the pattern is invalid.
//
// Examples:
//
//	keys.Fmt("USER#{id}")        // composite string
//	keys.Fmt("PROFILE")          // constant
//	keys.Fmt("{createdAt}")      // single field reference
func Fmt(pattern string) ValDef {
	s, err := parseFmtSpec(pattern, SpecKindS)
	if err != nil {
		panic(fmt.Sprintf("keys.Fmt: %v", err))
	}
	return ValDef{Format: &s}
}

// FromField creates a ValDef that copies directly from a field on the entity.
// Supports dot notation for nested fields (e.g., "user.id").
func FromField(fieldPath string) ValDef {
	return ValDef{FromField: fieldPath}
}

// parseFmtSpec parses a pattern string into a FmtSpec.
func parseFmtSpec(raw string, kind SpecKind) (FmtSpec, error) {
	if raw == "" {
		return FmtSpec{}, fmt.Errorf("pattern cannot be empty")
	}

	s := FmtSpec{raw: raw, kind: kind}

	// Find all field references
	matches := fieldRefRegex.FindAllStringSubmatchIndex(raw, -1)

	if len(matches) == 0 {
		// No field references - constant pattern
		s.parts = []specPart{{literal: true, value: raw}}
		return s, nil
	}

	lastEnd := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		fieldStart, fieldEnd := match[2], match[3]

		// Add literal part before this field reference (if any)
		if start > lastEnd {
			s.parts = append(s.parts, specPart{
				literal: true,
				value:   raw[lastEnd:start],
			})
		}

		// Add field reference
		fieldRef := raw[fieldStart:fieldEnd]
		if fieldRef == "" {
			return FmtSpec{}, fmt.Errorf("empty field reference at position %d", start)
		}

		// Validate field path components
		parts := strings.Split(fieldRef, ".")
		for i, part := range parts {
			if part == "" {
				return FmtSpec{}, fmt.Errorf("invalid field path %q: empty component at position %d", fieldRef, i)
			}
		}

		s.parts = append(s.parts, specPart{
			literal: false,
			value:   fieldRef,
		})

		lastEnd = end
	}

	// Add trailing literal (if any)
	if lastEnd < len(raw) {
		s.parts = append(s.parts, specPart{
			literal: true,
			value:   raw[lastEnd:],
		})
	}

	return s, nil
}

// String returns the raw pattern string.
func (s FmtSpec) String() string {
	return s.raw
}

// Kind returns the DynamoDB attribute type for this spec.
func (s FmtSpec) Kind() SpecKind {
	return s.kind
}

// IsConstant returns true if this spec has no field references.
func (s FmtSpec) IsConstant() bool {
	return len(s.parts) == 1 && s.parts[0].literal
}

// IsZero returns true if this is a zero-value (uninitialized) FmtSpec.
func (s FmtSpec) IsZero() bool {
	return s.raw == ""
}

// FieldRefs returns all field references in the pattern, in order.
// For "ORDER#{tenant}#{id}", returns ["tenant", "id"].
// For nested fields like "{user.id}", returns ["user.id"].
func (s FmtSpec) FieldRefs() []string {
	var refs []string
	for _, part := range s.parts {
		if !part.literal {
			refs = append(refs, part.value)
		}
	}
	return refs
}

// FieldPaths returns all field paths in the pattern, in order.
// Derived from FieldRefs by splitting on ".".
// For "ORDER#{tenant}#{id}", returns [["tenant"], ["id"]].
// For nested fields like "{user.id}", returns [["user", "id"]].
func (s FmtSpec) FieldPaths() [][]string {
	refs := s.FieldRefs()
	paths := make([][]string, len(refs))
	for i, ref := range refs {
		paths[i] = strings.Split(ref, ".")
	}
	return paths
}
