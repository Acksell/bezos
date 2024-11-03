package ddbstore

import (
	"fmt"
	"strconv"
)

func mustCast[T any](v any) T {
	out, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("type assertion failed, got %T want %T", v, out))
	}
	return out
}

func mustConvToString(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return mustCast[string](v)
}

func mustConvFloat64(v any) float64 {
	s := mustCast[string](v)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Errorf("parse float: %w", err))
	}
	return f
}
