package buckets

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

/*
BadgerDB can replicate dynamodb isolation guarantees
*/

type querier struct {
	db *badger.DB
}

func newQuerier(db *badger.DB) *querier {
	return &querier{db: db}
}

type queryResult struct {
	Items []any
}

func (s *querier) QueryPartition(pk string, opts badger.IteratorOptions) {

}

func (s *querier) QueryBetween(pk string, lower string, upper string, opts badger.IteratorOptions) (*queryResult, error) {
	results := &queryResult{}
	pagesize := 100 // todo parametrize
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		start := encodeKey(pk, lower)
		end := encodeKey(pk, upper)

		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil) // todo can reuse slice here
			if err != nil {
				return fmt.Errorf("copy value to result: %w", err)
			}
			results.Items = append(results.Items, val)
			k := item.Key()
			if string(k) >= string(end) {
				break // Stop at the upper bound
			}
			if len(results.Items) >= pagesize {
				break
			}
		}
		return nil
	})
	return results, err
}

func (s *querier) QueryBeginsWith(pk string, skPrefix string, opts badger.IteratorOptions) {

}

func (s *querier) QueryLessThan(pk, sk string, opts badger.IteratorOptions) {
	opts.Reverse = true
	// return s.QueryGreaterThan(pk, sk, opts)
}

func (s *querier) QueryLessThanEqual(pk, sk string, opts badger.IteratorOptions) {
	opts.Reverse = true
	// return s.QueryGreaterThanOrEqual(pk, sk, opts)
}

func (s *querier) QueryGreaterThanOrEqual(partitionKey string, sortKey string, opts badger.IteratorOptions) {

}

func (s *querier) QueryGreaterThan(pk, sk string, opts badger.IteratorOptions) {

}
