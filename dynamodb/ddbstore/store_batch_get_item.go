package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// BatchGetItem retrieves multiple items by their primary keys.
func (s *Store) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.RequestItems == nil {
		return nil, fmt.Errorf("request items is required")
	}

	response := &dynamodb.BatchGetItemOutput{
		Responses: make(map[string][]map[string]types.AttributeValue),
		// UnprocessedKeys is left nil - in a real DynamoDB this would contain
		// keys that couldn't be processed due to throughput limits. Since this
		// is a local store without throughput constraints, all keys are always processed.
		// todo: Simulate failures & unprocessedkeys
	}

	err := s.db.View(func(txn *badger.Txn) error {
		for tableName, keysAndAttrs := range params.RequestItems {
			tabl, err := s.getTable(&tableName)
			if err != nil {
				return err
			}

			for _, keyAttrs := range keysAndAttrs.Keys {
				pk, err := tabl.definition.ExtractPrimaryKey(keyAttrs)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				badgerItem, err := txn.Get(key)
				if err == badger.ErrKeyNotFound {
					continue // Item not found, skip
				}
				if err != nil {
					return err
				}

				var item map[string]types.AttributeValue
				if err := badgerItem.Value(func(val []byte) error {
					item, err = DeserializeItem(val)
					return err
				}); err != nil {
					return err
				}

				// Apply projection expression if specified
				item, err = projectionexpr.Project(keysAndAttrs.ProjectionExpression, keysAndAttrs.ExpressionAttributeNames, item)
				if err != nil {
					return err
				}

				response.Responses[tableName] = append(response.Responses[tableName], item)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}
