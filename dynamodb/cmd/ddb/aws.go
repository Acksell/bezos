package main

import (
	"context"
	"fmt"

	"github.com/acksell/bezos/dynamodb/schema"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// AWSOptions holds configuration for connecting to real AWS DynamoDB.
type AWSOptions struct {
	Region   string
	Profile  string
	Endpoint string
}

// createAWSClient creates a real AWS DynamoDB client using the standard
// credential chain (env vars, ~/.aws/credentials, IAM roles, SSO, etc.).
func createAWSClient(ctx context.Context, opts AWSOptions) (*dynamodb.Client, error) {
	var cfgOpts []func(*config.LoadOptions) error

	if opts.Region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(opts.Region))
	}
	if opts.Profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(opts.Profile))
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var ddbOpts []func(*dynamodb.Options)
	if opts.Endpoint != "" {
		ddbOpts = append(ddbOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = &opts.Endpoint
		})
	}

	return dynamodb.NewFromConfig(cfg, ddbOpts...), nil
}

// discoverAWSTables calls ListTables + DescribeTable for each table not already
// present in existingTables, and returns a schema.Schema containing the discovered tables.
func discoverAWSTables(ctx context.Context, client *dynamodb.Client, existingTables map[string]bool) ([]schema.Table, error) {
	// List all tables (paginated)
	var tableNames []string
	var lastTable *string
	for {
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			ExclusiveStartTableName: lastTable,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tables: %w", err)
		}
		tableNames = append(tableNames, out.TableNames...)
		if out.LastEvaluatedTableName == nil {
			break
		}
		lastTable = out.LastEvaluatedTableName
	}

	// Describe each table not already covered by schema files
	var discovered []schema.Table
	for _, name := range tableNames {
		if existingTables[name] {
			continue
		}

		desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: &name,
		})
		if err != nil {
			return nil, fmt.Errorf("describing table %s: %w", name, err)
		}

		t := describeTableToSchema(desc.Table)
		discovered = append(discovered, t)
	}

	return discovered, nil
}

// describeTableToSchema converts a DescribeTable output into a schema.Table.
func describeTableToSchema(desc *types.TableDescription) schema.Table {
	// Build attribute type lookup: attribute name -> kind (S, N, B)
	attrTypes := make(map[string]string)
	for _, attr := range desc.AttributeDefinitions {
		attrTypes[*attr.AttributeName] = string(attr.AttributeType)
	}

	t := schema.Table{
		Name: *desc.TableName,
	}

	// Extract primary key schema
	for _, ks := range desc.KeySchema {
		name := *ks.AttributeName
		kind := attrTypes[name]
		switch ks.KeyType {
		case types.KeyTypeHash:
			t.PartitionKey = schema.KeyDef{Name: name, Kind: kind}
		case types.KeyTypeRange:
			t.SortKey = &schema.KeyDef{Name: name, Kind: kind}
		}
	}

	// Extract GSIs
	for _, gsi := range desc.GlobalSecondaryIndexes {
		g := schema.GSI{
			Name: *gsi.IndexName,
		}
		for _, ks := range gsi.KeySchema {
			name := *ks.AttributeName
			kind := attrTypes[name]
			switch ks.KeyType {
			case types.KeyTypeHash:
				g.PartitionKey = schema.KeyDef{Name: name, Kind: kind}
			case types.KeyTypeRange:
				g.SortKey = &schema.KeyDef{Name: name, Kind: kind}
			}
		}
		t.GSIs = append(t.GSIs, g)
	}

	return t
}

// AWSAccountInfo holds the AWS account ID and alias (display name).
type AWSAccountInfo struct {
	AccountID string
	Alias     string
}

// getAWSAccountInfo calls STS GetCallerIdentity for the account ID and
// IAM ListAccountAliases for the account alias. Both are best-effort.
func getAWSAccountInfo(ctx context.Context, opts AWSOptions) AWSAccountInfo {
	var cfgOpts []func(*config.LoadOptions) error
	if opts.Region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(opts.Region))
	}
	if opts.Profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(opts.Profile))
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return AWSAccountInfo{}
	}

	var info AWSAccountInfo

	// Account ID via STS
	stsClient := sts.NewFromConfig(cfg)
	stsOut, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err == nil && stsOut.Account != nil {
		info.AccountID = *stsOut.Account
	}

	// Account alias via IAM (most accounts have at most one alias)
	iamClient := iam.NewFromConfig(cfg)
	iamOut, err := iamClient.ListAccountAliases(ctx, &iam.ListAccountAliasesInput{})
	if err == nil && len(iamOut.AccountAliases) > 0 {
		info.Alias = iamOut.AccountAliases[0]
	}

	return info
}
