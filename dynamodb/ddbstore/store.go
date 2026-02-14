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
	"github.com/acksell/bezos/dynamodb/ddbstore/updateexpr"
	"github.com/acksell/bezos/dynamodb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

// Store is a DynamoDB-compatible store backed by BadgerDB.
// It provides full ACID guarantees and supports all major DynamoDB operations.
type Store struct {
	db     *badger.DB
	tables map[string]*tableSchema
}

type tableSchema struct {
	definition table.TableDefinition
	gsis       map[string]*gsiSchema
}

func (t *tableSchema) encodeKey(pk table.PrimaryKey) ([]byte, error) {
	return encodeBadgerKey(t.definition.Name, "", pk)
}

type gsiSchema struct {
	tableName  string
	definition table.GSIDefinition
}

func (g *gsiSchema) encodeKey(pk table.PrimaryKey) ([]byte, error) {
	return encodeBadgerKey(g.tableName, g.definition.Name, pk)
}

// StoreOptions configures the BadgerDB store.
type StoreOptions struct {
	// Path to the database directory. If empty, uses in-memory mode.
	Path string
	// InMemory forces in-memory mode even if Path is set.
	InMemory bool
	// Logger for BadgerDB. If nil, logging is disabled.
	Logger badger.Logger
}

// New creates a new BadgerDB-backed DynamoDB store.
func New(opts StoreOptions, defs ...table.TableDefinition) (*Store, error) {
	badgerOpts := badger.DefaultOptions(opts.Path)

	if opts.Path == "" || opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	if opts.Logger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.Logger)
	} else {
		badgerOpts = badgerOpts.WithLogger(nil)
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("open badger db: %w", err)
	}

	tables := make(map[string]*tableSchema)
	for _, def := range defs {
		schema := &tableSchema{
			definition: def,
			gsis:       make(map[string]*gsiSchema),
		}

		for _, gsiDef := range def.GSIs {
			gsiSch := &gsiSchema{
				tableName:  def.Name,
				definition: gsiDef,
			}
			schema.gsis[gsiDef.Name] = gsiSch
		}

		tables[def.Name] = schema
	}

	return &Store{
		db:     db,
		tables: tables,
	}, nil
}

// Close closes the BadgerDB database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) getTable(tableName *string) (*tableSchema, error) {
	if tableName == nil {
		return nil, fmt.Errorf("table name is required")
	}
	schema, ok := s.tables[*tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", *tableName)
	}
	return schema, nil
}

// Used in query/scan to get the appropriate key encoder based on table and index name.
func (s *Store) getBadgerKeyEncoder(tableName *string, indexName *string) (*badgerKeyEncoder, error) {
	schema, err := s.getTable(tableName)
	if err != nil {
		return nil, err
	}
	if indexName == nil || *indexName == "" {
		return &badgerKeyEncoder{
			tableName: schema.definition.Name,
			indexName: "",
			keyDefs:   schema.definition.KeyDefinitions,
		}, nil
	}
	gsi, ok := schema.gsis[*indexName]
	if !ok {
		return nil, fmt.Errorf("GSI not found: %s", *indexName)
	}
	return &badgerKeyEncoder{
		tableName: gsi.tableName,
		indexName: gsi.definition.Name,
		keyDefs:   gsi.definition.KeyDefinitions,
	}, nil
}

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

// BatchWriteItem performs multiple put/delete operations.
func (s *Store) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.RequestItems == nil {
		return nil, fmt.Errorf("request items is required")
	}

	unprocessed := make(map[string][]types.WriteRequest)

	err := s.db.Update(func(txn *badger.Txn) error {
		for tableName, writeRequests := range params.RequestItems {
			tabl, err := s.getTable(&tableName)
			if err != nil {
				return err
			}

			for _, req := range writeRequests {
				switch {
				case req.PutRequest != nil:
					pk, err := tabl.definition.ExtractPrimaryKey(req.PutRequest.Item)
					if err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}

					key, err := tabl.encodeKey(pk)
					if err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}

					itemBytes, err := SerializeItem(req.PutRequest.Item)
					if err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}

					// Get old item for GSI maintenance
					var oldItem map[string]types.AttributeValue
					if badgerItem, err := txn.Get(key); err == nil {
						badgerItem.Value(func(val []byte) error {
							oldItem, _ = DeserializeItem(val)
							return nil
						})
					}

					if err := txn.Set(key, itemBytes); err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}

					// Update GSIs
					for _, gsi := range tabl.gsis {
						s.updateGSI(txn, gsi, req.PutRequest.Item, oldItem)
					}

				case req.DeleteRequest != nil:
					pk, err := tabl.definition.ExtractPrimaryKey(req.DeleteRequest.Key)
					if err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}

					key, err := tabl.encodeKey(pk)
					if err != nil {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
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
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
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

				default:
					return fmt.Errorf("empty write request")
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: unprocessed,
	}, nil
}

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

// Helper functions

func ptrStr(s string) *string {
	return &s
}

func attributeValuesEqual(a, b types.AttributeValue) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		if bv, ok := b.(*types.AttributeValueMemberS); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberN:
		if bv, ok := b.(*types.AttributeValueMemberN); ok {
			return av.Value == bv.Value
		}
	case *types.AttributeValueMemberB:
		if bv, ok := b.(*types.AttributeValueMemberB); ok {
			return bytes.Equal(av.Value, bv.Value)
		}
	}
	return false
}

func extractKeyAttributes(item map[string]types.AttributeValue, keyDef table.PrimaryKeyDefinition) map[string]types.AttributeValue {
	result := make(map[string]types.AttributeValue)
	if pk, ok := item[keyDef.PartitionKey.Name]; ok {
		result[keyDef.PartitionKey.Name] = pk
	}
	if keyDef.SortKey.Name != "" {
		if sk, ok := item[keyDef.SortKey.Name]; ok {
			result[keyDef.SortKey.Name] = sk
		}
	}
	return result
}

func incrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)
	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result
		}
		result[i] = 0
	}
	// Overflow - append 0x00
	return append(result, 0x00)
}
