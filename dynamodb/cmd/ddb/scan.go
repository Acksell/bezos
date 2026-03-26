package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/ddbcli"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func runScan() error {
	if len(os.Args) < 2 || os.Args[1] == "--help" || os.Args[1] == "-h" {
		printScanUsage()
		return nil
	}

	entityName := os.Args[1]
	if entityName == "help" {
		printScanUsage()
		return nil
	}

	// Everything after entity name is flags
	flagArgsScan := os.Args[2:]

	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	connFlags := RegisterConnectionFlags(fs)
	limit := fs.Int("limit", 0, "maximum number of items to return")
	gsiName := fs.String("gsi", "", "scan a Global Secondary Index")
	consistent := fs.Bool("consistent", false, "use strongly consistent read (not supported on GSIs)")
	if err := fs.Parse(flagArgsScan); err != nil {
		return err
	}

	// Load schemas and find entity
	schemas, err := loadSchemas()
	if err != nil {
		return err
	}

	match, ok := ddbcli.FindEntity(schemas, entityName)
	if !ok {
		return fmt.Errorf("entity %q not found in schema\n\nRun 'ddb schema entities' to see available entity types", entityName)
	}

	// Build a filter expression using the entity's PK prefix.
	// This ensures we only return items belonging to this entity type.
	pkPrefix := ddbcli.LiteralPrefix(match.Entity.PartitionKeyPattern)

	var filterExpr *string
	exprNames := map[string]string{}
	exprValues := map[string]types.AttributeValue{}

	if pkPrefix != "" {
		exprNames["#pk"] = match.Table.PartitionKey.Name
		exprValues[":pkprefix"] = makeAttributeValue(pkPrefix, match.Table.PartitionKey.Kind)
		expr := "begins_with(#pk, :pkprefix)"
		filterExpr = &expr
	}

	input := &dynamodb.ScanInput{
		TableName: &match.Table.Name,
	}

	if filterExpr != nil {
		input.FilterExpression = filterExpr
		input.ExpressionAttributeNames = exprNames
		input.ExpressionAttributeValues = exprValues
	}

	if *gsiName != "" {
		gsi := findGSI(match.Table, *gsiName)
		if gsi == nil {
			return fmt.Errorf("table %q has no GSI %q", match.Table.Name, *gsiName)
		}
		input.IndexName = gsiName
	}

	if *limit > 0 {
		input.Limit = aws.Int32(int32(*limit))
	}

	if *consistent {
		if *gsiName != "" {
			return fmt.Errorf("--consistent is not supported on GSI scans")
		}
		input.ConsistentRead = consistent
	}

	// Connect and execute
	ctx := context.Background()
	client, cleanup, err := connFlags.Connect(ctx, schemas)
	if err != nil {
		return err
	}
	defer cleanup()

	out, err := client.Scan(ctx, input)
	if err != nil {
		return fmt.Errorf("Scan: %w", err)
	}

	result := struct {
		Count        int32            `json:"count"`
		ScannedCount int32            `json:"scannedCount"`
		Items        []map[string]any `json:"items"`
	}{
		Count:        out.Count,
		ScannedCount: out.ScannedCount,
		Items:        ddbcli.ItemsToJSON(out.Items),
	}

	return writeJSONStdout(result)
}

func printScanUsage() {
	fmt.Println(`ddb scan - Scan items by entity type

Usage:
  ddb scan <EntityType> [flags]

The scan automatically filters to only return items matching the entity's
partition key prefix pattern.

Flags:
  --limit N         Maximum number of items to return
  --gsi NAME        Scan a Global Secondary Index
  --consistent      Use strongly consistent read (primary index only)
  --aws             Connect to AWS DynamoDB (default)
  --region STRING   AWS region
  --profile STRING  AWS profile name
  --endpoint URL    Custom DynamoDB endpoint
  --db PATH         Path to local database directory
  --memory          Use in-memory database

Examples:
  ddb scan User
  ddb scan User --limit 10
  ddb scan Order --limit 50
  ddb scan User --gsi GSI1 --limit 20
  ddb scan User --memory`)
}
