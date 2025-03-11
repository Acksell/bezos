package buckets

import (
	"github.com/dgraph-io/badger/v4"
)

// not a physical storage partition, just a logical one
type Partition struct {
	Key string

	*badger.DB
}
