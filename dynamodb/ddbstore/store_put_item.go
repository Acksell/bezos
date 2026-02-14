package ddbstore

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/ddbstore/conditionexpr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// PutItem creates or replaces an item.
func (s *Store) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Item == nil {
		return nil, fmt.Errorf("item is required")
	}

	tabl, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}

	pk, err := tabl.definition.ExtractPrimaryKey(params.Item)
	if err != nil {
		return nil, fmt.Errorf("extract primary key: %w", err)
	}

	key, err := tabl.encodeKey(pk)
	if err != nil {
		return nil, fmt.Errorf("encode key: %w", err)
	}

	itemBytes, err := SerializeItem(params.Item)
	if err != nil {
		return nil, fmt.Errorf("serialize item: %w", err)
	}

	var oldItem map[string]types.AttributeValue

	err = s.db.Update(func(txn *badger.Txn) error {
		// Check for existing item (for condition expression and return values)
		existingItem, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err != badger.ErrKeyNotFound {
			// Item exists - check condition expression
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
		} else if params.ConditionExpression != nil {
			// Item doesn't exist - evaluate condition against empty document
			input := conditionexpr.EvalInput{
				ExpressionValues: params.ExpressionAttributeValues,
				ExpressionNames:  params.ExpressionAttributeNames,
			}
			valid, err := conditionexpr.Eval(*params.ConditionExpression, input, nil)
			if err != nil {
				return fmt.Errorf("evaluate condition: %w", err)
			}
			if !valid {
				return &types.ConditionalCheckFailedException{
					Message: ptrStr("The conditional request failed"),
				}
			}
		}

		// Write the new item to main table
		if err := txn.Set(key, itemBytes); err != nil {
			return err
		}

		// Handle GSI updates
		for _, gsi := range tabl.gsis {
			if err := s.updateGSI(txn, gsi, params.Item, oldItem); err != nil {
				return fmt.Errorf("update GSI %s: %w", gsi.definition.Name, err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	out := &dynamodb.PutItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld && oldItem != nil {
		out.Attributes = oldItem
	}
	return out, nil
}

// updateGSI handles GSI maintenance during PutItem.
func (s *Store) updateGSI(txn *badger.Txn, gsi *gsiSchema, newItem, oldItem map[string]types.AttributeValue) error {
	pkAttr := gsi.definition.KeyDefinitions.PartitionKey.Name
	skAttr := gsi.definition.KeyDefinitions.SortKey.Name

	// Check if new item has GSI key attributes
	newPK, hasNewPK := newItem[pkAttr]
	var newSK types.AttributeValue
	if skAttr != "" {
		newSK = newItem[skAttr]
	}

	// Delete old GSI entry if keys changed
	if oldItem != nil {
		oldPK, hadOldPK := oldItem[pkAttr]
		var oldSK types.AttributeValue
		if skAttr != "" {
			oldSK = oldItem[skAttr]
		}

		if hadOldPK {
			// Check if GSI key changed
			keysChanged := !attributeValuesEqual(newPK, oldPK) || !attributeValuesEqual(newSK, oldSK)
			if keysChanged || !hasNewPK {
				// Delete old entry
				oldGSIPK, err := gsi.definition.ExtractPrimaryKey(oldItem)
				if err == nil { // Ignore errors - old item might not have had complete GSI key
					oldKey, err := gsi.encodeKey(oldGSIPK)
					if err == nil {
						txn.Delete(oldKey) // Ignore delete errors
					}
				}
			}
		}
	}

	// Insert new GSI entry if key attributes are present
	if hasNewPK {
		// For GSI with sort key, both must be present
		if skAttr != "" && newSK == nil {
			return nil // Skip - incomplete GSI key
		}

		gsiPK, err := gsi.definition.ExtractPrimaryKey(newItem)
		if err != nil {
			return nil // Skip - can't extract key (e.g., wrong type)
		}

		gsiKey, err := gsi.encodeKey(gsiPK)
		if err != nil {
			return fmt.Errorf("encode GSI key: %w", err)
		}

		// GSIs store the full item
		itemBytes, err := SerializeItem(newItem)
		if err != nil {
			return fmt.Errorf("serialize item for GSI: %w", err)
		}

		if err := txn.Set(gsiKey, itemBytes); err != nil {
			return err
		}
	}

	return nil
}
