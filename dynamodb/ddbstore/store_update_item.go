package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/conditionexpr"
	"github.com/acksell/bezos/dynamodb/ddbstore/updateexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// UpdateItem updates an existing item or creates a new one.
func (s *Store) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Key == nil {
		return nil, fmt.Errorf("key is required")
	}
	if params.UpdateExpression == nil {
		return nil, fmt.Errorf("UpdateExpression is required")
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

	// Parse the update expression
	updateExpr, err := updateexpr.Parse(*params.UpdateExpression)
	if err != nil {
		return nil, fmt.Errorf("parse update expression: %w", err)
	}

	var evalOutput *updateexpr.EvalOutput

	err = s.db.Update(func(txn *badger.Txn) error {
		// Get existing item
		var oldItem map[string]types.AttributeValue
		existingItem, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err != badger.ErrKeyNotFound {
			// Item exists
			if err := existingItem.Value(func(val []byte) error {
				oldItem, err = DeserializeItem(val)
				return err
			}); err != nil {
				return err
			}
		}

		// Evaluate condition expression if present
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

		// Prepare the base item with key attributes
		baseItem := make(map[string]types.AttributeValue)
		for k, v := range oldItem {
			baseItem[k] = v
		}
		for k, v := range params.Key {
			baseItem[k] = v
		}

		// Apply the update expression with return values handling
		evalInput := updateexpr.EvalInput{
			ExpressionNames:  params.ExpressionAttributeNames,
			ExpressionValues: params.ExpressionAttributeValues,
			ReturnValues:     params.ReturnValues,
		}
		evalOutput, err = updateexpr.Apply(updateExpr, evalInput, baseItem)
		if err != nil {
			return fmt.Errorf("apply update expression: %w", err)
		}

		// Serialize and write the updated item
		itemBytes, err := SerializeItem(evalOutput.Item)
		if err != nil {
			return fmt.Errorf("serialize item: %w", err)
		}

		if err := txn.Set(key, itemBytes); err != nil {
			return err
		}

		// Handle GSI updates
		for _, gsi := range tabl.gsis {
			if err := s.updateGSI(txn, gsi, evalOutput.Item, oldItem); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &dynamodb.UpdateItemOutput{
		// This is not thread safe:
		// we may not return the correct result
		// ALL_OLD - may return an older version.
		// ALL_NEW - may return a different value than the actual
		// UPDATED_OLD - may return an older version than the actual old value.
		// UPDATED_NEW - may return a different value than the actual updated value.
		Attributes: evalOutput.ReturnAttributes,
	}, nil
}
