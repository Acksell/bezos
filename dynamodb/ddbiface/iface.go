// Package ddbiface provides the core interface for DynamoDB client operations.
// This interface is satisfied by both the AWS SDK v2 DynamoDB client and by
// ddbstore.Store, allowing code to work with either real AWS DynamoDB or
// local disk/memory store.
package ddbiface

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client interface {
	ReadWriteClient
	AdminClient
}

// ReadWriter is the interface for DynamoDB client operations.
// It mirrors the method signatures of the AWS SDK v2 *dynamodb.Client.
type ReadWriteClient interface {
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	TransactGetItems(ctx context.Context, params *dynamodb.TransactGetItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)

	// PartiQL not supported for now (feature parity with normal API anyways)
	// BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
	// ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error)
	// ExecuteTransaction(ctx context.Context, params *dynamodb.ExecuteTransactionInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteTransactionOutput, error)
}

type AdminClient interface {
	// CreateBackup(ctx context.Context, params *dynamodb.CreateBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateBackupOutput, error)
	// CreateGlobalTable(ctx context.Context, params *dynamodb.CreateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateGlobalTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	// DeleteBackup(ctx context.Context, params *dynamodb.DeleteBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteBackupOutput, error)
	// DeleteResourcePolicy(ctx context.Context, params *dynamodb.DeleteResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteResourcePolicyOutput, error)
	DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	// DescribeBackup(ctx context.Context, params *dynamodb.DescribeBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeBackupOutput, error)
	// DescribeContinuousBackups(ctx context.Context, params *dynamodb.DescribeContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error)
	// DescribeContributorInsights(ctx context.Context, params *dynamodb.DescribeContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeContributorInsightsOutput, error)
	// DescribeEndpoints(ctx context.Context, params *dynamodb.DescribeEndpointsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeEndpointsOutput, error)
	// DescribeExport(ctx context.Context, params *dynamodb.DescribeExportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error)
	// DescribeGlobalTable(ctx context.Context, params *dynamodb.DescribeGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableOutput, error)
	// DescribeGlobalTableSettings(ctx context.Context, params *dynamodb.DescribeGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableSettingsOutput, error)
	// DescribeImport(ctx context.Context, params *dynamodb.DescribeImportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeImportOutput, error)
	// DescribeKinesisStreamingDestination(ctx context.Context, params *dynamodb.DescribeKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
	// DescribeLimits(ctx context.Context, params *dynamodb.DescribeLimitsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeLimitsOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	// DescribeTableReplicaAutoScaling(ctx context.Context, params *dynamodb.DescribeTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error)
	// DescribeTimeToLive(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	// DisableKinesisStreamingDestination(ctx context.Context, params *dynamodb.DisableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)
	// EnableKinesisStreamingDestination(ctx context.Context, params *dynamodb.EnableKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.EnableKinesisStreamingDestinationOutput, error)
	// ExportTableToPointInTime(ctx context.Context, params *dynamodb.ExportTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExportTableToPointInTimeOutput, error)
	// GetResourcePolicy(ctx context.Context, params *dynamodb.GetResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetResourcePolicyOutput, error)
	// ImportTable(ctx context.Context, params *dynamodb.ImportTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ImportTableOutput, error)
	// ListBackups(ctx context.Context, params *dynamodb.ListBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListBackupsOutput, error)
	// ListContributorInsights(ctx context.Context, params *dynamodb.ListContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListContributorInsightsOutput, error)
	// ListExports(ctx context.Context, params *dynamodb.ListExportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error)
	// ListGlobalTables(ctx context.Context, params *dynamodb.ListGlobalTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListGlobalTablesOutput, error)
	// ListImports(ctx context.Context, params *dynamodb.ListImportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListImportsOutput, error)
	ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
	// ListTagsOfResource(ctx context.Context, params *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
	// Options() dynamodb.Options
	// PutResourcePolicy(ctx context.Context, params *dynamodb.PutResourcePolicyInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutResourcePolicyOutput, error)
	// RestoreTableFromBackup(ctx context.Context, params *dynamodb.RestoreTableFromBackupInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableFromBackupOutput, error)
	// RestoreTableToPointInTime(ctx context.Context, params *dynamodb.RestoreTableToPointInTimeInput, optFns ...func(*dynamodb.Options)) (*dynamodb.RestoreTableToPointInTimeOutput, error)
	// TagResource(ctx context.Context, params *dynamodb.TagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error)
	// UntagResource(ctx context.Context, params *dynamodb.UntagResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error)
	// UpdateContinuousBackups(ctx context.Context, params *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
	// UpdateContributorInsights(ctx context.Context, params *dynamodb.UpdateContributorInsightsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContributorInsightsOutput, error)
	// UpdateGlobalTable(ctx context.Context, params *dynamodb.UpdateGlobalTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableOutput, error)
	// UpdateGlobalTableSettings(ctx context.Context, params *dynamodb.UpdateGlobalTableSettingsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableSettingsOutput, error)
	// UpdateKinesisStreamingDestination(ctx context.Context, params *dynamodb.UpdateKinesisStreamingDestinationInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateKinesisStreamingDestinationOutput, error)
	UpdateTable(ctx context.Context, params *dynamodb.UpdateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	// UpdateTableReplicaAutoScaling(ctx context.Context, params *dynamodb.UpdateTableReplicaAutoScalingInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error)
	UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
}
