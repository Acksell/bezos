package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/conditionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// DeleteItem removes an item by its primary key.
func (s *Store) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Key == nil {
		return nil, fmt.Errorf("key is required")
	}

	tabl, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}

	pk, err := tabl.definition.ExtractPrimaryKey(params.Key)
	if err != nil {
		return nil, fmt.Errorf("extract primary key: %w", err)
	}

	key, err := tabl.encodeKey(pk)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	var oldItem map[string]types.AttributeValue

	err = s.db.Update(func(txn *badger.Txn) error {
		// Get existing item for return values and GSI cleanup
		existingItem, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil // Nothing to delete
		}
		if err != nil {
			return err
		}

		if err := existingItem.Value(func(val []byte) error {
			oldItem, err = DeserializeItem(val)
			return err
		}); err != nil {
			return err
		}

		// Evaluate condition expression
		if params.ConditionExpression != nil {
			input := conditionexpr.EvalInput{
				ExpressionValues: params.ExpressionAttributeValues,
				ExpressionNames:  params.ExpressionAttributeNames,
			}
			valid, err := conditionexpr.Eval(*params.ConditionExpression, input, oldItem)
			if err != nil {
				return fmt.Errorf("evaluate condition: %w", err)
			}
			if !valid {
				return &types.ConditionalCheckFailedException{
					Message: ptrStr("The conditional request failed"),
				}
			}
		}

		// Delete from main table
		if err := txn.Delete(key); err != nil {
			return err
		}

		// Delete from GSIs
		for _, gsi := range tabl.gsis {
			pkAttr := gsi.definition.KeyDefinitions.PartitionKey.Name
			if _, hasPK := oldItem[pkAttr]; hasPK {
				gsiPK, err := gsi.definition.ExtractPrimaryKey(oldItem)
				if err == nil {
					gsiKey, err := gsi.encodeKey(gsiPK)
					if err == nil {
						txn.Delete(gsiKey) // Ignore errors
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	out := &dynamodb.DeleteItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld && oldItem != nil {
		out.Attributes = oldItem
	}
	return out, nil
}
