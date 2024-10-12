package astutil

import (
	"fmt"
	"strconv"
)

func CastTo[T any](v any, errctx string) T {
	out, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("%s: type assertion failed: got %T want %T", errctx, v, out))
	}
	return out
}

func Int(b []byte) int {
	i, err := strconv.Atoi(string(b))
	if err != nil {
		panic(err)
	}
	return i
}

func ToSlice[T any](v any, errctx string) []T {
	var out []T
	anySlice := v.([]any)

	for _, elem := range anySlice {
		t := CastTo[T](elem, errctx)
		out = append(out, t)
	}
	return out
}
