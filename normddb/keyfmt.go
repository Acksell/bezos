package normddb

import "strings"

func Keyfmt(parts ...string) string {
	return strings.Join(parts, "#")
}
