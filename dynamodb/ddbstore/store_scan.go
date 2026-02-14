package ddbstore

import (
	"bytes"
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/conditionexpr"
	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// Scan retrieves all items in a table, optionally with a filter.
func (s *Store) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}

	badgerEncoder, err := s.getBadgerKeyEncoder(params.TableName, params.IndexName)
	if err != nil {
		return nil, err
	}

	var items []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue

	limit := 0
	if params.Limit != nil {
		limit = int(*params.Limit)
	}

	prefix := badgerEncoder.tablePrefix()

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Handle pagination
		if params.ExclusiveStartKey != nil {
			startPK, err := badgerEncoder.keyDefs.ExtractPrimaryKey(params.ExclusiveStartKey)
			if err != nil {
				return fmt.Errorf("extract start key: %w", err)
			}
			startKey, err := badgerEncoder.encodeKey(startPK)
			if err != nil {
				return fmt.Errorf("encode start key: %w", err)
			}
			it.Seek(startKey)
			if it.Valid() {
				it.Next() // Skip the start key (exclusive)
			}
		} else {
			it.Seek(prefix)
		}

		for it.Valid() {
			if !bytes.HasPrefix(it.Item().Key(), prefix) {
				break
			}

			var item map[string]types.AttributeValue
			if err := it.Item().Value(func(val []byte) error {
				var err error
				item, err = DeserializeItem(val)
				return err
			}); err != nil {
				return err
			}

			// Apply filter expression if present
			if params.FilterExpression != nil {
				input := conditionexpr.EvalInput{
					ExpressionValues: params.ExpressionAttributeValues,
					ExpressionNames:  params.ExpressionAttributeNames,
				}
				matches, err := conditionexpr.Eval(*params.FilterExpression, input, item)
				if err != nil {
					return fmt.Errorf("evaluate filter: %w", err)
				}
				if !matches {
					it.Next()
					continue
				}
			}

			items = append(items, item)

			if limit > 0 && len(items) >= limit {
				lastKey = extractKeyAttributes(item, badgerEncoder.keyDefs)
				break
			}

			it.Next()
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Apply projection expression to results
	items, err = projectionexpr.ProjectAll(params.ProjectionExpression, params.ExpressionAttributeNames, items)
	if err != nil {
		return nil, err
	}

	count := int32(len(items))
	return &dynamodb.ScanOutput{
		Items:            items,
		Count:            count,
		ScannedCount:     count,
		LastEvaluatedKey: lastKey,
	}, nil
}
