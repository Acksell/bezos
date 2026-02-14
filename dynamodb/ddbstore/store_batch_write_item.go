package ddbstore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dgraph-io/badger/v4"
)

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
