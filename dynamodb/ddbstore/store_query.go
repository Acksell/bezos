package ddbstore

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbstore/conditionexpr"
	"github.com/acksell/bezos/dynamodb/ddbstore/keyconditionexpr"
	"github.com/acksell/bezos/dynamodb/ddbstore/keyconditionexpr/ast"
	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// Query retrieves items matching a key condition expression.
func (s *Store) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.KeyConditionExpression == nil {
		return nil, fmt.Errorf("key condition expression is required")
	}

	badgerEncoder, err := s.getBadgerKeyEncoder(params.TableName, params.IndexName)
	if err != nil {
		return nil, err
	}

	// Parse the key condition expression
	keyCond, err := keyconditionexpr.Parse(*params.KeyConditionExpression, keyconditionexpr.ParseParams{
		ExpressionAttributeNames:  params.ExpressionAttributeNames,
		ExpressionAttributeValues: params.ExpressionAttributeValues,
		TableKeys:                 badgerEncoder.keyDefs,
	})
	if err != nil {
		return nil, fmt.Errorf("parse key condition: %w", err)
	}

	// Get the partition key value
	pkValue := keyCond.PartitionKeyCond.EqualsValue.GetValue()

	// Build the prefix for this partition
	prefix, err := badgerEncoder.encodePartitionPrefix(pkValue.Value)
	if err != nil {
		return nil, fmt.Errorf("encode partition key prefix: %w", err)
	}

	var items []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue

	limit := 0
	if params.Limit != nil {
		limit = int(*params.Limit)
	}

	scanForward := params.ScanIndexForward == nil || *params.ScanIndexForward

	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = !scanForward
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Determine start position
		startKey := prefix
		if params.ExclusiveStartKey != nil {
			startPK, err := badgerEncoder.keyDefs.ExtractPrimaryKey(params.ExclusiveStartKey)
			if err != nil {
				return fmt.Errorf("extract start key: %w", err)
			}
			startKey, err = badgerEncoder.encodeKey(startPK)
			if err != nil {
				return fmt.Errorf("encode start key: %w", err)
			}
		}

		if !scanForward {
			// For reverse iteration, seek to end of prefix range
			endPrefix := incrementBytes(prefix)
			if params.ExclusiveStartKey != nil {
				it.Seek(startKey)
			} else {
				it.Seek(endPrefix)
			}
		} else {
			it.Seek(startKey)
			// Skip the start key if it's an exclusive start
			if params.ExclusiveStartKey != nil && it.Valid() {
				it.Next()
			}
		}

		for it.Valid() {
			if !bytes.HasPrefix(it.Item().Key(), prefix) {
				break
			}

			// Apply sort key condition if present
			if keyCond.SortKeyCond != nil {
				matches, err := s.matchesSortKeyCondition(it.Item().Key(), prefix, keyCond.SortKeyCond)
				if err != nil {
					return err
				}
				if !matches {
					// For range conditions, we might be able to stop early
					if s.canStopEarly(keyCond.SortKeyCond, scanForward) {
						break
					}
					it.Next()
					continue
				}
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
				// Set LastEvaluatedKey for pagination
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
	return &dynamodb.QueryOutput{
		Items:            items,
		Count:            count,
		ScannedCount:     count, // In real DDB, this might differ due to filters
		LastEvaluatedKey: lastKey,
	}, nil
}

// matchesSortKeyCondition checks if a key matches the sort key condition.
func (s *Store) matchesSortKeyCondition(fullKey, prefix []byte, cond *ast.SortKeyCondition) (bool, error) {
	// Extract the sort key portion from the full key
	skBytes := fullKey[len(prefix):]

	// Decode the sort key value
	skValue, err := decodeSortKeyValue(skBytes)
	if err != nil {
		return false, err
	}

	switch {
	case cond.Compare != nil:
		condValue := cond.Compare.Value.GetValue()
		return s.compareKeyValues(skValue, cond.Compare.Comp, &condValue), nil

	case cond.Between != nil:
		lower := cond.Between.Lower.GetValue()
		upper := cond.Between.Upper.GetValue()
		return skValue.GreaterThanOrEqual(&lower) && skValue.LessThanOrEqual(&upper), nil

	case cond.BeginsWith != nil:
		prefixVal := cond.BeginsWith.Prefix.GetValue()
		skStr, ok := skValue.Value.(string)
		if !ok {
			return false, fmt.Errorf("begins_with requires string sort key")
		}
		prefixStr, ok := prefixVal.Value.(string)
		if !ok {
			return false, fmt.Errorf("begins_with prefix must be string")
		}
		return strings.HasPrefix(skStr, prefixStr), nil
	}

	return true, nil
}

func decodeSortKeyValue(skBytes []byte) (*ast.KeyValue, error) {
	if len(skBytes) == 0 {
		return &ast.KeyValue{}, nil
	}

	keyType := skBytes[0]
	valueBytes := skBytes[1:]

	switch keyType {
	case keyTypeString:
		return &ast.KeyValue{
			Value: string(unescapeBytes(valueBytes)),
			Type:  ast.STRING,
		}, nil
	case keyTypeNumber:
		numStr, err := decodeNumber(valueBytes) // pass just the encoded number, not type byte
		if err != nil {
			return nil, err
		}
		return &ast.KeyValue{
			Value: numStr,
			Type:  ast.NUMBER,
		}, nil
	case keyTypeBinary:
		return &ast.KeyValue{
			Value: unescapeBytes(valueBytes),
			Type:  ast.BINARY,
		}, nil
	default:
		return nil, fmt.Errorf("unknown key type: %c", keyType)
	}
}

func (s *Store) compareKeyValues(left *ast.KeyValue, comp ast.KeyComparator, right *ast.KeyValue) bool {
	switch comp {
	case ast.Equal:
		return left.Equal(right)
	case ast.LessThan:
		return left.LessThan(right)
	case ast.LessOrEqual:
		return left.LessThanOrEqual(right)
	case ast.GreaterThan:
		return left.GreaterThan(right)
	case ast.GreaterOrEqual:
		return left.GreaterThanOrEqual(right)
	default:
		return false
	}
}

func (s *Store) canStopEarly(cond *ast.SortKeyCondition, scanForward bool) bool {
	// For certain conditions, we can stop iterating early
	if cond.Compare != nil {
		switch cond.Compare.Comp {
		case ast.LessThan, ast.LessOrEqual:
			return scanForward // Can stop when we exceed the upper bound
		case ast.GreaterThan, ast.GreaterOrEqual:
			return !scanForward // Can stop in reverse when below lower bound
		}
	}
	return false
}
