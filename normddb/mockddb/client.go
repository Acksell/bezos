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

func (c *mockStore) extractPrimaryKey(item map[string]types.AttributeValue, tableName string) (table.PrimaryKey, error) {
	tbl, found := c.tables[tableName]
	if !found {
		return table.PrimaryKey{}, fmt.Errorf("table not found")
	}
	keydef := tbl.definition.KeyDefinitions
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
func (c *mockStore) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	return nil, nil
}

func (c *mockStore) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return nil, nil
}

func (c *mockStore) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, nil
}

func (c *mockStore) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, nil
}

func (c *mockStore) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
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
	if params.TableName == nil {
		return nil, fmt.Errorf("table name is required")
	}
	tableName := *params.TableName

	table, ok := c.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}

	pk, err := c.extractPrimaryKey(params.Item, tableName)
	if err != nil {
		return nil, err
	}

	old := table.store[pk.Values.PartitionKey].ReplaceOrInsert(&document{pk, params.Item})

	out := &dynamodb.PutItemOutput{}
	if params.ReturnValues == types.ReturnValueAllOld {
		out.Attributes = old.(*document).value
	}
	return out, nil
}

func (c *mockStore) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	return nil, nil
}

func (c *mockStore) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, nil
}

func (c *mockStore) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return nil, nil
}

func (c *mockStore) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, nil
}

func (c *mockStore) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, nil
}

// All methods of dynamodb.Client.
// func (c *mockStore) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
// func (c *mockStore) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
// func (c *mockStore) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
// func (c *mockStore) CreateBackup(ctx context.Context, params *dynamodb.CreateBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateBackupOutput, error)
// func (c *mockStore) CreateGlobalTable(ctx context.Context, params *dynamodb.CreateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateGlobalTableOutput, error)
// func (c *mockStore) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
// func (c *mockStore) DeleteBackup(ctx context.Context, params *dynamodb.DeleteBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteBackupOutput, error)
// func (c *mockStore) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
// func (c *mockStore) DeleteResourcePolicy(ctx context.Context, params *dynamodb.DeleteResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteResourcePolicyOutput, error)
// func (c *mockStore) DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
// func (c *mockStore) DescribeBackup(ctx context.Context, params *dynamodb.DescribeBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeBackupOutput, error)
// func (c *mockStore) DescribeContinuousBackups(ctx context.Context, params *dynamodb.DescribeContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error)
// func (c *mockStore) DescribeContributorInsights(ctx context.Context, params *dynamodb.DescribeContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContributorInsightsOutput, error)
// func (c *mockStore) DescribeEndpoints(ctx context.Context, params *dynamodb.DescribeEndpointsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeEndpointsOutput, error)
// func (c *mockStore) DescribeExport(ctx context.Context, params *dynamodb.DescribeExportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error)
// func (c *mockStore) DescribeGlobalTable(ctx context.Context, params *dynamodb.DescribeGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableOutput, error)
// func (c *mockStore) DescribeGlobalTableSettings(ctx context.Context, params *dynamodb.DescribeGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableSettingsOutput, error)
// func (c *mockStore) DescribeImport(ctx context.Context, params *dynamodb.DescribeImportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeImportOutput, error)
// func (c *mockStore) DescribeKinesisStreamingDestination(ctx context.Context, params *dynamodb.DescribeKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
// func (c *mockStore) DescribeLimits(ctx context.Context, params *dynamodb.DescribeLimitsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeLimitsOutput, error)
// func (c *mockStore) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
// func (c *mockStore) DescribeTableReplicaAutoScaling(ctx context.Context, params *dynamodb.DescribeTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error)
// func (c *mockStore) DescribeTimeToLive(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
// func (c *mockStore) DisableKinesisStreamingDestination(ctx context.Context, params *dynamodb.DisableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)
// func (c *mockStore) EnableKinesisStreamingDestination(ctx context.Context, params *dynamodb.EnableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.EnableKinesisStreamingDestinationOutput, error)
// func (c *mockStore) ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error)
// func (c *mockStore) ExecuteTransaction(ctx context.Context, params *dynamodb.ExecuteTransactionInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteTransactionOutput, error)
// func (c *mockStore) ExportTableToPointInTime(ctx context.Context, params *dynamodb.ExportTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExportTableToPointInTimeOutput, error)
// func (c *mockStore) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
// func (c *mockStore) GetResourcePolicy(ctx context.Context, params *dynamodb.GetResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetResourcePolicyOutput, error)
// func (c *mockStore) ImportTable(ctx context.Context, params *dynamodb.ImportTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ImportTableOutput, error)
// func (c *mockStore) ListBackups(ctx context.Context, params *dynamodb.ListBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListBackupsOutput, error)
// func (c *mockStore) ListContributorInsights(ctx context.Context, params *dynamodb.ListContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListContributorInsightsOutput, error)
// func (c *mockStore) ListExports(ctx context.Context, params *dynamodb.ListExportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error)
// func (c *mockStore) ListGlobalTables(ctx context.Context, params *dynamodb.ListGlobalTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListGlobalTablesOutput, error)
// func (c *mockStore) ListImports(ctx context.Context, params *dynamodb.ListImportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListImportsOutput, error)
// func (c *mockStore) ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
// func (c *mockStore) ListTagsOfResource(ctx context.Context, params *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
// func (c *mockStore) Options() dynamodb.Options
// func (c *mockStore) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
// func (c *mockStore) PutResourcePolicy(ctx context.Context, params *dynamodb.PutResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutResourcePolicyOutput, error)
// func (c *mockStore) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
// func (c *mockStore) RestoreTableFromBackup(ctx context.Context, params *dynamodb.RestoreTableFromBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableFromBackupOutput, error)
// func (c *mockStore) RestoreTableToPointInTime(ctx context.Context, params *dynamodb.RestoreTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableToPointInTimeOutput, error)
// func (c *mockStore) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
// func (c *mockStore) TagResource(ctx context.Context, params *dynamodb.TagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error)
// func (c *mockStore) TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
// func (c *mockStore) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
// func (c *mockStore) UntagResource(ctx context.Context, params *dynamodb.UntagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error)
// func (c *mockStore) UpdateContinuousBackups(ctx context.Context, params *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
// func (c *mockStore) UpdateContributorInsights(ctx context.Context, params *dynamodb.UpdateContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContributorInsightsOutput, error)
// func (c *mockStore) UpdateGlobalTable(ctx context.Context, params *dynamodb.UpdateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableOutput, error)
// func (c *mockStore) UpdateGlobalTableSettings(ctx context.Context, params *dynamodb.UpdateGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableSettingsOutput, error)
// func (c *mockStore) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
// func (c *mockStore) UpdateKinesisStreamingDestination(ctx context.Context, params *dynamodb.UpdateKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error)
// func (c *mockStore) UpdateTable(ctx context.Context, params *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
// func (c *mockStore) UpdateTableReplicaAutoScaling(ctx context.Context, params *dynamodb.UpdateTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error)
// func (c *mockStore) UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
