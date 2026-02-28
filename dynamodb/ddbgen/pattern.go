package ddbgen

import (
	"fmt"
	"regexp"
	"strings"
)

// =============================================================================
// Key pattern parsing
// =============================================================================

type fmtSpec struct {
	Raw   string
	Kind  string
	Parts []specPart
}

type specPart struct {
	IsLiteral  bool
	Value      string
	Formats    []string
	PrintfSpec string
}

func (p specPart) format() string {
	if len(p.Formats) == 0 {
		return ""
	}
	return p.Formats[len(p.Formats)-1]
}

func (p specPart) hasModifier(mod string) bool {
	for _, f := range p.Formats {
		if f == mod {
			return true
		}
	}
	return false
}

func (p specPart) paramName() string {
	parts := strings.Split(p.Value, ".")
	return parts[len(parts)-1]
}

var fieldRefRegex = regexp.MustCompile(`\{([^}]*)\}`)

func parseFmtSpec(raw, kind string) (*fmtSpec, error) {
	if raw == "" {
		return nil, fmt.Errorf("pattern cannot be empty")
	}
	s := &fmtSpec{Raw: raw, Kind: kind}
	matches := fieldRefRegex.FindAllStringSubmatchIndex(raw, -1)
	if len(matches) == 0 {
		s.Parts = []specPart{{IsLiteral: true, Value: raw}}
		return s, nil
	}
	lastEnd := 0
	for _, match := range matches {
		start, end := match[0], match[1]
		fieldStart, fieldEnd := match[2], match[3]
		if start > lastEnd {
			s.Parts = append(s.Parts, specPart{IsLiteral: true, Value: raw[lastEnd:start]})
		}
		fullRef := raw[fieldStart:fieldEnd]
		if fullRef == "" {
			return nil, fmt.Errorf("empty field reference at position %d", start)
		}
		fieldPath, formats, printfSpec := parseFieldRef(fullRef)
		s.Parts = append(s.Parts, specPart{
			IsLiteral:  false,
			Value:      fieldPath,
			Formats:    formats,
			PrintfSpec: printfSpec,
		})
		lastEnd = end
	}
	if lastEnd < len(raw) {
		s.Parts = append(s.Parts, specPart{IsLiteral: true, Value: raw[lastEnd:]})
	}
	return s, nil
}

func parseFieldRef(ref string) (fieldPath string, formats []string, printfSpec string) {
	parts := strings.Split(ref, ":")
	fieldPath = parts[0]
	if len(parts) == 1 {
		return
	}
	lastPart := parts[len(parts)-1]
	if strings.HasPrefix(lastPart, "%") {
		printfSpec = lastPart
		formats = parts[1 : len(parts)-1]
	} else {
		formats = parts[1:]
	}
	return
}

func (s fmtSpec) isConstant() bool {
	return len(s.Parts) == 1 && s.Parts[0].IsLiteral
}

func (s fmtSpec) literalPrefix() string {
	if len(s.Parts) > 0 && s.Parts[0].IsLiteral {
		return s.Parts[0].Value
	}
	return ""
}

func (s fmtSpec) fieldRefs() []specPart {
	var refs []specPart
	for _, part := range s.Parts {
		if !part.IsLiteral {
			refs = append(refs, part)
		}
	}
	return refs
}

// =============================================================================
// Type helpers
// =============================================================================

func isIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64":
		return true
	}
	return false
}

func isSignedIntegerType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

func isFloatType(t string) bool {
	return t == "float32" || t == "float64"
}

func isTimeType(t string) bool {
	return t == "time.Time" || t == "Time"
}

func hasPadding(spec string) bool {
	return strings.HasPrefix(spec, "%0") && len(spec) > 2
}

func goParamType(fieldType string) string {
	if fieldType == "string" {
		return "string"
	}
	if isTimeType(fieldType) {
		return "time.Time"
	}
	return fieldType
}
