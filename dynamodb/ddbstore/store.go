package ddbstore

import (
	"bezos/dynamodb/ddbstore/expressions/writeconditions"
	"bezos/dynamodb/table"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/google/btree"
)

var errNotFound = fmt.Errorf("not found")

func NewStore(defs ...table.TableDefinition) *mockStore {
	tables := make(map[string]*mockTable)
	for _, t := range defs {
		gsis := make(map[string]*mockTable)
		for _, gsi := range t.GSIs {
			gsiTable := &mockTable{
				definition: gsi,
				store:      make(map[partitionKey]*btree.BTreeG[*document]),
			}
			gsis[gsi.Name] = gsiTable
		}
		tables[t.Name] = &mockTable{
			definition: t,
			gsis:       gsis,
			store:      make(map[partitionKey]*btree.BTreeG[*document]),
		}
	}
	return &mockStore{
		tables: tables,
	}
}

type mockStore struct {
	tables map[string]*mockTable
}

func (s *mockStore) getTable(tableName *string) (*mockTable, error) {
	if tableName == nil {
		return nil, fmt.Errorf("table name is required")
	}
	table, ok := s.tables[*tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s, %v", *tableName, s.tables)
	}
	return table, nil
}

type mockTable struct {
	definition table.TableDefinition
	// A store with partition key as primary key and a btree for sort keys.
	// Probably overkill but the library wasn't too big, and API works well.
	store map[partitionKey]*btree.BTreeG[*document]
	gsis  map[string]*mockTable
}

type partitionKey any

type document struct {
	pk    table.PrimaryKey
	value map[string]types.AttributeValue
}

func less(l *document, r *document) bool {
	if l.pk.Definition.SortKey.Kind != r.pk.Definition.SortKey.Kind {
		panic("sort key kind mismatch") // should not happen
	}
	switch l.pk.Definition.SortKey.Kind {
	case table.KeyKindS, table.KeyKindB:
		return mustConvToString(l.pk.Values.SortKey) < mustConvToString(r.pk.Values.SortKey)
	case table.KeyKindN:
		return mustConvFloat64(l.pk.Values.SortKey) < mustConvFloat64(r.pk.Values.SortKey)
	default:
		panic("unsupported key kind")
	}
}

func (t *mockTable) extractPrimaryKey(item map[string]types.AttributeValue) (table.PrimaryKey, error) {
	pk, err := t.definition.ExtractPrimaryKey(item) // todo maybe this function should be in this package instead
	if err != nil {
		return table.PrimaryKey{}, err
	}
	return pk, nil
}

func (t *mockTable) getStore(pk partitionKey) *btree.BTreeG[*document] {
	store, ok := t.store[pk]
	if !ok {
		store = btree.NewG(2, less)
		t.store[pk] = store
	}
	return store
}

// A subset of methods dealing with IO on documents. Not including PartiQL statements.
func (s *mockStore) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.RequestItems == nil {
		return nil, fmt.Errorf("request items is required")
	}
	response := &dynamodb.BatchGetItemOutput{
		Responses:       make(map[string][]map[string]types.AttributeValue),
		UnprocessedKeys: make(map[string]types.KeysAndAttributes),
	}
	for tableName, keys := range params.RequestItems {
		table, err := s.getTable(&tableName)
		if err != nil {
			return nil, err
		}
		for _, key := range keys.Keys {
			pk, err := table.extractPrimaryKey(key)
			if err != nil {
				return nil, err
			}
			store := table.getStore(pk.Values.PartitionKey)
			doc, found := store.Get(&document{pk, nil})
			if found {
				// TODO: handle projection expression
				// TODO: handle consistent read option
				response.Responses[tableName] = append(response.Responses[tableName], doc.value)
			} else {
				// Retrieve the current value, modify it, and put it back
				unprocessedKeys := response.UnprocessedKeys[tableName]
				unprocessedKeys.Keys = append(unprocessedKeys.Keys, key)
				response.UnprocessedKeys[tableName] = unprocessedKeys
			}
		}
	}

	return nil, nil
}

func (s *mockStore) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.RequestItems == nil {
		return nil, fmt.Errorf("request items is required")
	}
	unprocessed := make(map[string][]types.WriteRequest)
	for tableName, items := range params.RequestItems {
		table, err := s.getTable(&tableName)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			switch {
			case item.PutRequest != nil:
				_, err := table.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: &tableName,
					Item:      item.PutRequest.Item,
				})
				if err != nil {
					unprocessed[tableName] = append(unprocessed[tableName], item)
				}
			case item.DeleteRequest != nil:
				_, err := table.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: &tableName,
					Key:       item.DeleteRequest.Key,
				})
				if err != nil {
					unprocessed[tableName] = append(unprocessed[tableName], item)
				}
			default:
				return nil, fmt.Errorf("empty write request, must be put or delete")
			}
		}
	}
	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: unprocessed,
	}, nil
}

func (s *mockStore) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	table, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}
	return table.DeleteItem(ctx, params, optFns...)
}

func (t *mockTable) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Key == nil {
		return nil, fmt.Errorf("key is required")
	}

	pk, err := t.extractPrimaryKey(params.Key)
	if err != nil {
		return nil, err
	}

	store := t.getStore(pk.Values.PartitionKey)
	old, _ := store.Delete(&document{pk, nil})

	if t.definition.IsGSI { // if it's a GSI we can return early here.
		return nil, nil
	}

	for _, gsi := range t.gsis {
		gsi.DeleteItem(ctx, params, optFns...)
	}

	out := &dynamodb.DeleteItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld {
		out.Attributes = old.value
	}
	return out, nil
}

func (s *mockStore) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	table, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}
	return table.GetItem(ctx, params, optFns...)
}

func (t *mockTable) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Key == nil {
		return nil, fmt.Errorf("key is required")
	}

	pk, err := t.extractPrimaryKey(params.Key)
	if err != nil {
		return nil, err
	}

	store := t.getStore(pk.Values.PartitionKey)

	doc, found := store.Get(&document{pk, nil})
	if !found {
		return &dynamodb.GetItemOutput{}, errNotFound
	}
	return &dynamodb.GetItemOutput{Item: doc.value}, nil
}

func (s *mockStore) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	table, err := s.getTable(params.TableName)
	if err != nil {
		return nil, err
	}
	return table.PutItem(ctx, params, optFns...)
}

func (t *mockTable) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.Item == nil {
		return nil, fmt.Errorf("item is required")
	}
	pk, err := t.extractPrimaryKey(params.Item)
	if err != nil {
		return nil, err
	}

	store := t.getStore(pk.Values.PartitionKey)
	// todo make concurrency safe - make topic queues
	doc, found := store.Get(&document{pk, nil})

	// only validate if document is found
	if found && params.ConditionExpression != nil && !t.definition.IsGSI { // no need to do validation again for gsi, main table does it
		input := writeconditions.EvalInput{
			ExpressionValues: params.ExpressionAttributeValues,
			ExpressionNames:  params.ExpressionAttributeNames,
		}
		valid, err := writeconditions.Eval(*params.ConditionExpression, input, doc.value)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, fmt.Errorf("condition failed: %v %w", valid, err)
		}
	}
	old, _ := store.ReplaceOrInsert(&document{pk, params.Item})

	if t.definition.IsGSI { // if it's a GSI we can return early here.
		return nil, nil
	}

	// if gsi key attribute is changed, we need to delete the item from the gsi
	// if gsi key attribute is present, we need to Put the item to the gsi
	for _, gsi := range t.gsis {
		newPk, hasNewPk := params.Item[gsi.definition.KeyDefinitions.PartitionKey.Name]
		if hasNewPk { // Always put item if PK is present.
			if _, err := gsi.PutItem(ctx, params, optFns...); err != nil {
				return nil, fmt.Errorf("put to gsi: %w", err)
			}
		}
		if old == nil {
			continue
		}
		var newSk, oldSk types.AttributeValue
		if gsi.definition.KeyDefinitions.SortKey.Name != "" {
			newSk = params.Item[gsi.definition.KeyDefinitions.SortKey.Name]
			oldSk = old.value[gsi.definition.KeyDefinitions.SortKey.Name]
		}
		oldPk := old.value[gsi.definition.KeyDefinitions.PartitionKey.Name]
		// todo test
		if newPk != oldPk || newSk != oldSk { // GSI primary key changed or deleted, delete old document.
			oldKey, err := gsi.extractPrimaryKey(old.value)
			if err != nil {
				return nil, err
			}
			// todo: any conditions needed here?
			if _, err := gsi.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: params.TableName, Key: oldKey.DDB()}, optFns...); err != nil {
				return nil, fmt.Errorf("delete from gsi: %w", err)
			}
		}
	}

	out := &dynamodb.PutItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld {
		out.Attributes = old.value
	}
	return out, nil
}

func (s *mockStore) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if params == nil {
		return nil, fmt.Errorf("params is required")
	}
	if params.TransactItems == nil {
		return nil, fmt.Errorf("transact items is required")
	}
	response := &dynamodb.TransactGetItemsOutput{
		Responses:        make([]types.ItemResponse, 0),
		ConsumedCapacity: make([]types.ConsumedCapacity, 0),
	}
	for _, item := range params.TransactItems {
		if item.Get == nil {
			return nil, fmt.Errorf("empty transact get item request")
		}
		table, err := s.getTable(item.Get.TableName)
		if err != nil {
			return nil, err
		}
		pk, err := table.extractPrimaryKey(item.Get.Key)
		if err != nil {
			return nil, err
		}
		store := table.getStore(pk.Values.PartitionKey)
		doc, found := store.Get(&document{pk, nil})
		if found {
			response.Responses = append(response.Responses, types.ItemResponse{
				Item: doc.value,
			})
		}
	}
	return response, nil
}

func (s *mockStore) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, nil
}

func (s *mockStore) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return nil, nil
}

func (s *mockStore) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, nil
}

func (s *mockStore) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, nil
}

// All methods of dynamodb.Client.
// func (s *mockStore) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
// func (s *mockStore) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
// func (s *mockStore) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
// func (s *mockStore) CreateBackup(ctx context.Context, params *dynamodb.CreateBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateBackupOutput, error)
// func (s *mockStore) CreateGlobalTable(ctx context.Context, params *dynamodb.CreateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateGlobalTableOutput, error)
// func (s *mockStore) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
// func (s *mockStore) DeleteBackup(ctx context.Context, params *dynamodb.DeleteBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteBackupOutput, error)
// func (s *mockStore) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
// func (s *mockStore) DeleteResourcePolicy(ctx context.Context, params *dynamodb.DeleteResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteResourcePolicyOutput, error)
// func (s *mockStore) DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
// func (s *mockStore) DescribeBackup(ctx context.Context, params *dynamodb.DescribeBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeBackupOutput, error)
// func (s *mockStore) DescribeContinuousBackups(ctx context.Context, params *dynamodb.DescribeContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error)
// func (s *mockStore) DescribeContributorInsights(ctx context.Context, params *dynamodb.DescribeContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContributorInsightsOutput, error)
// func (s *mockStore) DescribeEndpoints(ctx context.Context, params *dynamodb.DescribeEndpointsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeEndpointsOutput, error)
// func (s *mockStore) DescribeExport(ctx context.Context, params *dynamodb.DescribeExportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error)
// func (s *mockStore) DescribeGlobalTable(ctx context.Context, params *dynamodb.DescribeGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableOutput, error)
// func (s *mockStore) DescribeGlobalTableSettings(ctx context.Context, params *dynamodb.DescribeGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableSettingsOutput, error)
// func (s *mockStore) DescribeImport(ctx context.Context, params *dynamodb.DescribeImportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeImportOutput, error)
// func (s *mockStore) DescribeKinesisStreamingDestination(ctx context.Context, params *dynamodb.DescribeKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
// func (s *mockStore) DescribeLimits(ctx context.Context, params *dynamodb.DescribeLimitsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeLimitsOutput, error)
// func (s *mockStore) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
// func (s *mockStore) DescribeTableReplicaAutoScaling(ctx context.Context, params *dynamodb.DescribeTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error)
// func (s *mockStore) DescribeTimeToLive(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
// func (s *mockStore) DisableKinesisStreamingDestination(ctx context.Context, params *dynamodb.DisableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)
// func (s *mockStore) EnableKinesisStreamingDestination(ctx context.Context, params *dynamodb.EnableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.EnableKinesisStreamingDestinationOutput, error)
// func (s *mockStore) ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error)
// func (s *mockStore) ExecuteTransaction(ctx context.Context, params *dynamodb.ExecuteTransactionInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteTransactionOutput, error)
// func (s *mockStore) ExportTableToPointInTime(ctx context.Context, params *dynamodb.ExportTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExportTableToPointInTimeOutput, error)
// func (s *mockStore) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
// func (s *mockStore) GetResourcePolicy(ctx context.Context, params *dynamodb.GetResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetResourcePolicyOutput, error)
// func (s *mockStore) ImportTable(ctx context.Context, params *dynamodb.ImportTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ImportTableOutput, error)
// func (s *mockStore) ListBackups(ctx context.Context, params *dynamodb.ListBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListBackupsOutput, error)
// func (s *mockStore) ListContributorInsights(ctx context.Context, params *dynamodb.ListContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListContributorInsightsOutput, error)
// func (s *mockStore) ListExports(ctx context.Context, params *dynamodb.ListExportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error)
// func (s *mockStore) ListGlobalTables(ctx context.Context, params *dynamodb.ListGlobalTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListGlobalTablesOutput, error)
// func (s *mockStore) ListImports(ctx context.Context, params *dynamodb.ListImportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListImportsOutput, error)
// func (s *mockStore) ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
// func (s *mockStore) ListTagsOfResource(ctx context.Context, params *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
// func (s *mockStore) Options() dynamodb.Options
// func (s *mockStore) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
// func (s *mockStore) PutResourcePolicy(ctx context.Context, params *dynamodb.PutResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutResourcePolicyOutput, error)
// func (s *mockStore) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
// func (s *mockStore) RestoreTableFromBackup(ctx context.Context, params *dynamodb.RestoreTableFromBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableFromBackupOutput, error)
// func (s *mockStore) RestoreTableToPointInTime(ctx context.Context, params *dynamodb.RestoreTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableToPointInTimeOutput, error)
// func (s *mockStore) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
// func (s *mockStore) TagResource(ctx context.Context, params *dynamodb.TagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error)
// func (s *mockStore) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
// func (s *mockStore) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
// func (s *mockStore) UntagResource(ctx context.Context, params *dynamodb.UntagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error)
// func (s *mockStore) UpdateContinuousBackups(ctx context.Context, params *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
// func (s *mockStore) UpdateContributorInsights(ctx context.Context, params *dynamodb.UpdateContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContributorInsightsOutput, error)
// func (s *mockStore) UpdateGlobalTable(ctx context.Context, params *dynamodb.UpdateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableOutput, error)
// func (s *mockStore) UpdateGlobalTableSettings(ctx context.Context, params *dynamodb.UpdateGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableSettingsOutput, error)
// func (s *mockStore) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
// func (s *mockStore) UpdateKinesisStreamingDestination(ctx context.Context, params *dynamodb.UpdateKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error)
// func (s *mockStore) UpdateTable(ctx context.Context, params *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
// func (s *mockStore) UpdateTableReplicaAutoScaling(ctx context.Context, params *dynamodb.UpdateTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error)
// func (s *mockStore) UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
