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

// TransactWriteItems performs multiple write operations atomically.
func (s *Store) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.TransactItems == nil {
		return nil, fmt.Errorf("transact items is required")
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		// First pass: validate all conditions
		for i, item := range params.TransactItems {
			switch {
			case item.Put != nil:
				if item.Put.ConditionExpression != nil {
					tabl, err := s.getTable(item.Put.TableName)
					if err != nil {
						return err
					}

					pk, err := tabl.definition.ExtractPrimaryKey(item.Put.Item)
					if err != nil {
						return err
					}

					key, err := tabl.encodeKey(pk)
					if err != nil {
						return err
					}

					var existingItem map[string]types.AttributeValue
					if badgerItem, err := txn.Get(key); err == nil {
						badgerItem.Value(func(val []byte) error {
							existingItem, _ = DeserializeItem(val)
							return nil
						})
					}

					input := conditionexpr.EvalInput{
						ExpressionValues: item.Put.ExpressionAttributeValues,
						ExpressionNames:  item.Put.ExpressionAttributeNames,
					}
					valid, err := conditionexpr.Eval(*item.Put.ConditionExpression, input, existingItem)
					if err != nil {
						return fmt.Errorf("item %d: evaluate condition: %w", i, err)
					}
					if !valid {
						return &types.TransactionCanceledException{
							Message: ptrStr(fmt.Sprintf("Transaction cancelled, condition check failed for item %d", i)),
						}
					}
				}

			case item.Delete != nil:
				if item.Delete.ConditionExpression != nil {
					tabl, err := s.getTable(item.Delete.TableName)
					if err != nil {
						return err
					}

					pk, err := tabl.definition.ExtractPrimaryKey(item.Delete.Key)
					if err != nil {
						return err
					}

					key, err := tabl.encodeKey(pk)
					if err != nil {
						return err
					}

					var existingItem map[string]types.AttributeValue
					if badgerItem, err := txn.Get(key); err == nil {
						badgerItem.Value(func(val []byte) error {
							existingItem, _ = DeserializeItem(val)
							return nil
						})
					}

					input := conditionexpr.EvalInput{
						ExpressionValues: item.Delete.ExpressionAttributeValues,
						ExpressionNames:  item.Delete.ExpressionAttributeNames,
					}
					valid, err := conditionexpr.Eval(*item.Delete.ConditionExpression, input, existingItem)
					if err != nil {
						return fmt.Errorf("item %d: evaluate condition: %w", i, err)
					}
					if !valid {
						return &types.TransactionCanceledException{
							Message: ptrStr(fmt.Sprintf("Transaction cancelled, condition check failed for item %d", i)),
						}
					}
				}

			case item.ConditionCheck != nil:
				tabl, err := s.getTable(item.ConditionCheck.TableName)
				if err != nil {
					return err
				}

				pk, err := tabl.definition.ExtractPrimaryKey(item.ConditionCheck.Key)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				var existingItem map[string]types.AttributeValue
				if badgerItem, err := txn.Get(key); err == nil {
					badgerItem.Value(func(val []byte) error {
						existingItem, _ = DeserializeItem(val)
						return nil
					})
				}

				input := conditionexpr.EvalInput{
					ExpressionValues: item.ConditionCheck.ExpressionAttributeValues,
					ExpressionNames:  item.ConditionCheck.ExpressionAttributeNames,
				}
				valid, err := conditionexpr.Eval(*item.ConditionCheck.ConditionExpression, input, existingItem)
				if err != nil {
					return fmt.Errorf("item %d: evaluate condition: %w", i, err)
				}
				if !valid {
					return &types.TransactionCanceledException{
						Message: ptrStr(fmt.Sprintf("Transaction cancelled, condition check failed for item %d", i)),
					}
				}

			case item.Update != nil:
				tabl, err := s.getTable(item.Update.TableName)
				if err != nil {
					return err
				}

				pk, err := tabl.definition.ExtractPrimaryKey(item.Update.Key)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				var existingItem map[string]types.AttributeValue
				if badgerItem, err := txn.Get(key); err == nil {
					badgerItem.Value(func(val []byte) error {
						existingItem, _ = DeserializeItem(val)
						return nil
					})
				}

				// Evaluate condition expression if present
				if item.Update.ConditionExpression != nil {
					input := conditionexpr.EvalInput{
						ExpressionValues: item.Update.ExpressionAttributeValues,
						ExpressionNames:  item.Update.ExpressionAttributeNames,
					}
					valid, err := conditionexpr.Eval(*item.Update.ConditionExpression, input, existingItem)
					if err != nil {
						return fmt.Errorf("item %d: evaluate condition: %w", i, err)
					}
					if !valid {
						return &types.TransactionCanceledException{
							Message: ptrStr(fmt.Sprintf("Transaction cancelled, condition check failed for item %d", i)),
						}
					}
				}
			}
		}

		// Second pass: perform all writes
		for _, item := range params.TransactItems {
			switch {
			case item.Put != nil:
				tabl, err := s.getTable(item.Put.TableName)
				if err != nil {
					return err
				}

				pk, err := tabl.definition.ExtractPrimaryKey(item.Put.Item)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				// Get old item for GSI maintenance
				var oldItem map[string]types.AttributeValue
				if badgerItem, err := txn.Get(key); err == nil {
					badgerItem.Value(func(val []byte) error {
						oldItem, _ = DeserializeItem(val)
						return nil
					})
				}

				itemBytes, err := SerializeItem(item.Put.Item)
				if err != nil {
					return err
				}

				if err := txn.Set(key, itemBytes); err != nil {
					return err
				}

				// Update GSIs
				for _, gsi := range tabl.gsis {
					if err := s.updateGSI(txn, gsi, item.Put.Item, oldItem); err != nil {
						return err
					}
				}

			case item.Delete != nil:
				tabl, err := s.getTable(item.Delete.TableName)
				if err != nil {
					return err
				}

				pk, err := tabl.definition.ExtractPrimaryKey(item.Delete.Key)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				// Get old item for GSI cleanup
				var oldItem map[string]types.AttributeValue
				if badgerItem, err := txn.Get(key); err == nil {
					badgerItem.Value(func(val []byte) error {
						oldItem, _ = DeserializeItem(val)
						return nil
					})
				}

				if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
					return err
				}

				// Delete from GSIs
				if oldItem != nil {
					for _, gsi := range tabl.gsis {
						pkAttr := gsi.definition.KeyDefinitions.PartitionKey.Name
						if _, hasPK := oldItem[pkAttr]; hasPK {
							if gsiPK, err := gsi.definition.ExtractPrimaryKey(oldItem); err == nil {
								if gsiKey, err := gsi.encodeKey(gsiPK); err == nil {
									txn.Delete(gsiKey)
								}
							}
						}
					}
				}

			case item.ConditionCheck != nil:
				// Already validated, nothing to write

			case item.Update != nil:
				tabl, err := s.getTable(item.Update.TableName)
				if err != nil {
					return err
				}

				pk, err := tabl.definition.ExtractPrimaryKey(item.Update.Key)
				if err != nil {
					return err
				}

				key, err := tabl.encodeKey(pk)
				if err != nil {
					return err
				}

				// Get existing item
				var oldItem map[string]types.AttributeValue
				if badgerItem, err := txn.Get(key); err == nil {
					badgerItem.Value(func(val []byte) error {
						oldItem, _ = DeserializeItem(val)
						return nil
					})
				}

				// Parse and apply the update expression
				updateExpr, err := updateexpr.Parse(*item.Update.UpdateExpression)
				if err != nil {
					return fmt.Errorf("parse update expression: %w", err)
				}

				// Start with existing item or empty, ensure key attributes
				baseItem := make(map[string]types.AttributeValue)
				for k, v := range oldItem {
					baseItem[k] = v
				}
				for k, v := range item.Update.Key {
					baseItem[k] = v
				}

				// Apply the update expression
				evalInput := updateexpr.EvalInput{
					ExpressionNames:  item.Update.ExpressionAttributeNames,
					ExpressionValues: item.Update.ExpressionAttributeValues,
				}
				evalOutput, err := updateexpr.Apply(updateExpr, evalInput, baseItem)
				if err != nil {
					return fmt.Errorf("apply update expression: %w", err)
				}

				// Serialize and write
				itemBytes, err := SerializeItem(evalOutput.Item)
				if err != nil {
					return err
				}

				if err := txn.Set(key, itemBytes); err != nil {
					return err
				}

				// Update GSIs
				for _, gsi := range tabl.gsis {
					if err := s.updateGSI(txn, gsi, evalOutput.Item, oldItem); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &dynamodb.TransactWriteItemsOutput{}, nil
}
