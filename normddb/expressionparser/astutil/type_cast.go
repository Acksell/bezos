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

func Int(v any) int {
	return CastTo[int](v)
}

func String(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
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
	anySlice, ok := v.([]any)
	if !ok {
		panic(fmt.Sprintf("%s: type assertion failed: got %T want []any", strings.Join(errctx, " "), v))
	}
	for _, elem := range anySlice {
		t := CastTo[T](elem, errctx...)
		out = append(out, t)
	}
	return out
}

func HeadTailString(head any, tail any) string {
	hb := CastTo[[]byte](head, "astutil.HeadTailString hb")
	s := string(hb)
	if tail == nil {
		return s
	}

	tb := ToSlice[[]byte](tail, "astutil.HeadTailString tb")
	for _, b := range tb {
		s += string(b)
	}
	return s
}

func Join(group any) string {
	return strings.Join(ToSlice[string](group, "astutil.Join"), "")
}

func HeadTailList(head any, tail any) []any {
	return append([]any{head}, ToSlice[any](tail, "astutil.HeadTailList")...)
}
