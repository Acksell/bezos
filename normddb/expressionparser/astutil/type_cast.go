package astutil

import (
	"fmt"
	"strconv"
	"strings"
)

func CastTo[T any](v any, errctx ...string) T {
	out, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("%s: type assertion failed: got %T want %T", strings.Join(errctx, " "), v, out))
	}
	return out
}

func String(v any) string {
	return CastTo[string](v)
}

func Atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}

// numbers are stored as strings
func Float64(v any) float64 {
	s := CastTo[string](v)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Errorf("parse float: %w", err))
	}
	return f
}

func ToSlice[T any](v any, errctx ...string) []T {
	var out []T
	anySlice := v.([]any)

	for _, elem := range anySlice {
		t := CastTo[T](elem, errctx...)
		out = append(out, t)
	}
	return out
}

func HeadTailString(head any, tail any) string {
	return CastTo[string](head) + Join(tail)
}

func Join(group any) string {
	return strings.Join(ToSlice[string](group), "")
}

func HeadTailList(head any, tail any) []string {
	return append([]string{CastTo[string](head)}, ToSlice[string](tail)...)
}
