package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbcli"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func runGet() error {
	if len(os.Args) < 2 || os.Args[1] == "--help" || os.Args[1] == "-h" {
		printGetUsage()
		return nil
	}

	entityName := os.Args[1]
	if entityName == "help" {
		printGetUsage()
		return nil
	}

	// Separate key=value args from flags
	kvArgs, flagArgs := splitKVAndFlags(os.Args[2:])

	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	connFlags := RegisterConnectionFlags(fs)
	consistent := fs.Bool("consistent", false, "use strongly consistent read")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	// Parse key=value pairs
	values, err := parseKVArgs(kvArgs)
	if err != nil {
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

	// Validate all required params are provided
	params := ddbcli.RequiredParams(match.Entity)
	if err := validateParams(params, values); err != nil {
		return err
	}

	// Build the key
	key, err := buildEntityKey(match, values)
	if err != nil {
		return err
	}

	// Connect and execute
	ctx := context.Background()
	client, cleanup, err := connFlags.Connect(ctx, schemas)
	if err != nil {
		return err
	}
	defer cleanup()

	input := &dynamodb.GetItemInput{
		TableName: &match.Table.Name,
		Key:       key,
	}
	if *consistent {
		input.ConsistentRead = consistent
	}

	out, err := client.GetItem(ctx, input)
	if err != nil {
		return fmt.Errorf("GetItem: %w", err)
	}

	if out.Item == nil {
		fmt.Fprintln(os.Stderr, "item not found")
		os.Exit(1)
	}

	return writeJSONStdout(ddbcli.ItemToJSON(out.Item))
}

// buildEntityKey constructs the DynamoDB key map from entity patterns and user values.
func buildEntityKey(match ddbcli.EntityMatch, values map[string]string) (map[string]types.AttributeValue, error) {
	entity := match.Entity
	table := match.Table

	pkValue, err := ddbcli.BuildKeyFromEntity(entity.PartitionKeyPattern, entity, values)
	if err != nil {
		return nil, fmt.Errorf("building partition key: %w", err)
	}

	key := make(map[string]types.AttributeValue)
	key[table.PartitionKey.Name] = makeAttributeValue(pkValue, table.PartitionKey.Kind)

	if table.SortKey != nil && entity.SortKeyPattern != "" {
		skValue, err := ddbcli.BuildKeyFromEntity(entity.SortKeyPattern, entity, values)
		if err != nil {
			return nil, fmt.Errorf("building sort key: %w", err)
		}
		key[table.SortKey.Name] = makeAttributeValue(skValue, table.SortKey.Kind)
	}

	return key, nil
}

// makeAttributeValue creates an AttributeValue of the given kind (S, N, B).
func makeAttributeValue(value string, kind string) types.AttributeValue {
	switch kind {
	case "N":
		return &types.AttributeValueMemberN{Value: value}
	case "B":
		b, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			// If not valid base64, use raw bytes
			b = []byte(value)
		}
		return &types.AttributeValueMemberB{Value: b}
	default: // "S"
		return &types.AttributeValueMemberS{Value: value}
	}
}

// splitKVAndFlags separates key=value positional args from flag args (starting with -).
func splitKVAndFlags(args []string) (kvArgs, flagArgs []string) {
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
		} else if strings.Contains(arg, "=") {
			kvArgs = append(kvArgs, arg)
		} else {
			// Unknown positional arg — treat as flag arg for error reporting
			flagArgs = append(flagArgs, arg)
		}
	}
	return
}

// parseKVArgs parses key=value strings into a map.
func parseKVArgs(args []string) (map[string]string, error) {
	values := make(map[string]string, len(args))
	for _, arg := range args {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			return nil, fmt.Errorf("invalid argument %q (expected key=value)", arg)
		}
		if k == "" {
			return nil, fmt.Errorf("empty key in %q", arg)
		}
		values[k] = v
	}
	return values, nil
}

// validateParams checks that all required parameters have been provided.
func validateParams(params []ddbcli.Param, values map[string]string) error {
	var missing []string
	for _, p := range params {
		if _, ok := values[p.Name]; !ok {
			missing = append(missing, p.Name+" ("+p.Type+")")
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func printGetUsage() {
	fmt.Println(`ddb get - Get an item by entity type and key fields

Usage:
  ddb get <EntityType> <field=value> [...] [flags]

Flags:
  --consistent      Use strongly consistent read
  --aws             Connect to AWS DynamoDB (default)
  --region STRING   AWS region
  --profile STRING  AWS profile name
  --endpoint URL    Custom DynamoDB endpoint
  --db PATH         Path to local database directory
  --memory          Use in-memory database

Examples:
  ddb get User id=abc123
  ddb get Order tenantID=tenant-42 orderID=order-1
  ddb get User id=abc123 --consistent
  ddb get User id=abc123 --memory`)
}
