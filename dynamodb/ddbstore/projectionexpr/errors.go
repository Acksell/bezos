package projectionexpr

import "fmt"

// UnresolvedAttributeNameError is returned when an expression attribute name
// cannot be resolved from the provided names map.
type UnresolvedAttributeNameError struct {
	Alias string
}

func (e *UnresolvedAttributeNameError) Error() string {
	return fmt.Sprintf("unresolved expression attribute name %q", e.Alias)
}
