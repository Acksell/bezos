package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/projectionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// TransactGetItems retrieves multiple items atomically.
func (s *Store) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.TransactItems == nil {
		return nil, fmt.Errorf("transact items is required")
	}

	response := &dynamodb.TransactGetItemsOutput{
		Responses: make([]types.ItemResponse, 0, len(params.TransactItems)),
	}

	err := s.db.View(func(txn *badger.Txn) error {
		for _, item := range params.TransactItems {
			if item.Get == nil {
				return fmt.Errorf("empty transact get item request")
			}

			tabl, err := s.getTable(item.Get.TableName)
			if err != nil {
				return err
			}

			pk, err := tabl.definition.ExtractPrimaryKey(item.Get.Key)
			if err != nil {
				return err
			}

			key, err := tabl.encodeKey(pk)
			if err != nil {
				return err
			}

			badgerItem, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				response.Responses = append(response.Responses, types.ItemResponse{})
				continue
			}
			if err != nil {
				return err
			}

			var docItem map[string]types.AttributeValue
			if err := badgerItem.Value(func(val []byte) error {
				docItem, err = DeserializeItem(val)
				return err
			}); err != nil {
				return err
			}

			// Apply projection expression if specified
			docItem, err = projectionexpr.Project(item.Get.ProjectionExpression, item.Get.ExpressionAttributeNames, docItem)
			if err != nil {
				return err
			}

			response.Responses = append(response.Responses, types.ItemResponse{Item: docItem})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}
