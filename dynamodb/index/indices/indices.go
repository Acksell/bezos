// Package indices provides a registry for PrimaryIndex definitions.
//
// This package is intentionally minimal with no codegen dependencies.
// Use it in your application code to register indices:
//
//	var _ = indices.Add(index.PrimaryIndex[User]{
//	    Table:        UserTable,
//	    PartitionKey: val.Fmt("USER#{id}"),
//	})
//
// Then retrieve indices in your application:
//
//	idx := indices.Get[User]()
//	pk := idx.PrimaryKey(userId)
//
// For code generation, the ddbgen/indexgen package reads from this registry.
package indices

import (
	"reflect"
	"sync"

	"github.com/acksell/bezos/dynamodb/index"
)

// Entry holds a registered PrimaryIndex (stored as any to preserve the generic type).
type Entry struct {
	EntityType reflect.Type // The entity type E
	Index      any          // *index.PrimaryIndex[E]
}

var (
	mu       sync.RWMutex
	registry = make(map[reflect.Type]Entry)
	order    []reflect.Type // preserves registration order for deterministic codegen
)

// Add registers a PrimaryIndex for the entity type E.
// Returns a pointer to the stored index for use in application code.
//
// Example:
//
//	var _ = indices.Add(index.PrimaryIndex[User]{...})
func Add[E any](idx index.PrimaryIndex[E]) *index.PrimaryIndex[E] {
	p := &idx
	t := reflect.TypeOf((*E)(nil)).Elem()

	mu.Lock()
	defer mu.Unlock()

	if _, exists := registry[t]; !exists {
		order = append(order, t)
	}
	registry[t] = Entry{
		EntityType: t,
		Index:      p,
	}
	return p
}

// Get retrieves the registered PrimaryIndex for entity type E.
// Panics if no index is registered for the type.
//
// Example:
//
//	idx := indices.Get[User]()
func Get[E any]() *index.PrimaryIndex[E] {
	t := reflect.TypeOf((*E)(nil)).Elem()

	mu.RLock()
	defer mu.RUnlock()

	entry, ok := registry[t]
	if !ok {
		panic("indices: no index registered for type " + t.String())
	}
	return entry.Index.(*index.PrimaryIndex[E])
}

// All returns all registered entries in registration order.
// Used by code generation tools.
func All() []Entry {
	mu.RLock()
	defer mu.RUnlock()

	result := make([]Entry, len(order))
	for i, t := range order {
		result[i] = registry[t]
	}
	return result
}

// Clear resets the registry. Useful for testing.
func Clear() {
	mu.Lock()
	defer mu.Unlock()
	registry = make(map[reflect.Type]Entry)
	order = nil
}
