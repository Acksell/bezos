package mockddb

import (
	"context"
	"fmt"
	"norm/normddb/expressionparser"
	"norm/normddb/table"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/google/btree"
)

func NewStore(defs ...table.TableDefinition) *mockStore {
	tables := make(map[string]mockTable)
	for _, t := range defs {
		tables[t.Name] = mockTable{
			definition: t,
			store:      make(map[partitionKey]*btree.BTree),
		}
	}
	return &mockStore{
		tables: make(map[string]*mockTable),
	}
}

type mockStore struct {
	tables map[string]*mockTable
}

type mockTable struct {
	definition table.TableDefinition
	// A store with partition key as primary key and a btree for sort keys.
	// Probably overkill but the library wasn't too big.
	store map[partitionKey]*btree.BTree
	gsis  []*mockTable
}

type partitionKey any

type document struct {
	pk    table.PrimaryKey
	value map[string]types.AttributeValue
}

func (d document) Less(than btree.Item) bool {
	other, ok := than.(document)
	if !ok {
		panic("btree entry is not a document")
	}
	switch d.pk.Definition.SortKey.Kind {
	case table.KeyKindS, table.KeyKindB:
		return mustConvToString(d.pk.Values.SortKey) < mustConvToString(other.pk.Values.SortKey)
	case table.KeyKindN:
		return mustConvFloat64(d.pk.Values.SortKey) < mustConvFloat64(other.pk.Values.SortKey)
	default:
		panic("unsupported key kind")
	}
}

func (t *mockTable) extractPrimaryKey(item map[string]types.AttributeValue) (table.PrimaryKey, error) {
	keydef := t.definition.KeyDefinitions
	part, ok := item[keydef.PartitionKey.Name]
	if !ok {
		return table.PrimaryKey{}, fmt.Errorf("partition key not found")
	}
	sort, ok := item[keydef.SortKey.Name]
	if !ok && keydef.SortKey.Name != "" {
		return table.PrimaryKey{}, fmt.Errorf("sort key not found")
	}
	pk := table.PrimaryKey{
		Definition: keydef,
		Values: table.PrimaryKeyValues{
			PartitionKey: part,
			SortKey:      sort,
		},
	}
	return pk, nil
}

// A subset of methods dealing with IO on documents. Not including PartiQL statements.
func (s *mockStore) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	return nil, nil
}

func (s *mockStore) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return nil, nil
}

func (s *mockStore) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, nil
}

func (s *mockStore) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, nil
}

func (s *mockStore) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if params.TableName == nil {
		return nil, fmt.Errorf("table name is required")
	}
	tableName := *params.TableName

	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
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

	if params.ConditionExpression != nil {
		condition := expressionparser.Condition{
			Condition:        *params.ConditionExpression,
			ExpressionValues: params.ExpressionAttributeValues,
			ExpressionNames:  params.ExpressionAttributeNames,
		}
		valid, err := expressionparser.ValidateCondition(condition, params.Item)
		if err != nil {
			return nil, err
		}
		if !valid {
			return nil, fmt.Errorf("condition failed")
		}
	}

	pk, err := t.extractPrimaryKey(params.Item)
	if err != nil {
		return nil, err
	}

	old := t.store[pk.Values.PartitionKey].ReplaceOrInsert(&document{pk, params.Item})

	for _, gsi := range t.gsis {
		gsi.PutItem(ctx, params, optFns...)
	}

	out := &dynamodb.PutItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld {
		out.Attributes = old.(*document).value
	}
	return out, nil
}

func (s *mockStore) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	return nil, nil
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
