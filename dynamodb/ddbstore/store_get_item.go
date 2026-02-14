package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// GetItem retrieves a single item by its primary key.
func (s *Store) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Key == nil {
		return nil, fmt.Errorf("key is required")
	}

	t, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}

	pk, err := t.definition.ExtractPrimaryKey(params.Key)
	if err != nil {
		return nil, fmt.Errorf("extract primary key: %w", err)
	}

	key, err := t.encodeKey(pk)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	var item map[string]types.AttributeValue
	err = s.db.View(func(txn *badger.Txn) error {
		badgerItem, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return errNotFound
		}
		if err != nil {
			return err
		}
		return badgerItem.Value(func(val []byte) error {
			item, err = DeserializeItem(val)
			return err
		})
	})

	if err == errNotFound {
		return &dynamodb.GetItemOutput{}, errNotFound
	}
	if err != nil {
		return nil, err
	}

	// Apply projection expression if specified
	item, err = projectionexpr.Project(params.ProjectionExpression, params.ExpressionAttributeNames, item)
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemOutput{Item: item}, nil
}
