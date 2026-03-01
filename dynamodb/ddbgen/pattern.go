package ddbgen

import (
	"strings"
)

// =============================================================================
// Type helpers for code generation
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
