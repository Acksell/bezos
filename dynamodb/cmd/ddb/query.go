package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbcli"
	"github.com/acksell/bezos/dynamodb/schema"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// skCondition represents a parsed sort key condition from user input.
type skCondition struct {
	field string
	op    string // "=", "^=", ">", ">=", "<", "<="
	value string
}

// parseSkCondition parses a key=value arg that may contain an operator.
// Supported: field=val, field^=val, field>=val, field>val, field<=val, field<val
func parseSkCondition(arg string) (field string, cond *skCondition, isPlain bool) {
	// Try operators in order of specificity (longest first)
	for _, op := range []string{"^=", ">=", "<=", ">", "<", "="} {
		idx := strings.Index(arg, op)
		if idx > 0 {
			f := arg[:idx]
			v := arg[idx+len(op):]
			if op == "=" {
				// Plain equality — could be PK or SK
				return f, &skCondition{field: f, op: "=", value: v}, true
			}
			return f, &skCondition{field: f, op: op, value: v}, false
		}
	}
	return "", nil, false
}

func runQuery() error {
	if len(os.Args) < 2 || os.Args[1] == "--help" || os.Args[1] == "-h" {
		printQueryUsage()
		return nil
	}

	entityName := os.Args[1]
	if entityName == "help" {
		printQueryUsage()
		return nil
	}

	// Separate key=value args from flags
	kvArgs, flagArgs := splitKVAndFlags(os.Args[2:])

	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	connFlags := RegisterConnectionFlags(fs)
	gsiName := fs.String("gsi", "", "query a Global Secondary Index")
	limit := fs.Int("limit", 0, "maximum number of items to return")
	reverse := fs.Bool("reverse", false, "scan in reverse (descending sort key order)")
	consistent := fs.Bool("consistent", false, "use strongly consistent read (not supported on GSIs)")
	if err := fs.Parse(flagArgs); err != nil {
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

	// Determine which index we're querying
	var pkParams, skParams []ddbcli.Param
	var pkAttrName, skAttrName string
	var pkKind, skKind string
	var skPattern string
	indexName := "" // empty = primary index

	if *gsiName != "" {
		indexName = *gsiName
		pkParams, skParams, err = ddbcli.GSIParams(match.Entity, *gsiName)
		if err != nil {
			return err
		}

		// Find GSI definition for attribute names and types
		gsi := findGSI(match.Table, *gsiName)
		if gsi == nil {
			return fmt.Errorf("table %q has no GSI %q", match.Table.Name, *gsiName)
		}
		pkAttrName = gsi.PartitionKey.Name
		pkKind = gsi.PartitionKey.Kind
		if gsi.SortKey != nil {
			skAttrName = gsi.SortKey.Name
			skKind = gsi.SortKey.Kind
		}

		// Find GSI mapping for sort key pattern
		for _, m := range match.Entity.GSIMappings {
			if strings.EqualFold(m.GSI, *gsiName) {
				skPattern = m.SortPattern
				break
			}
		}
	} else {
		pkParams = ddbcli.RequiredPKParams(match.Entity)
		skParams = ddbcli.RequiredSKParams(match.Entity)
		pkAttrName = match.Table.PartitionKey.Name
		pkKind = match.Table.PartitionKey.Kind
		if match.Table.SortKey != nil {
			skAttrName = match.Table.SortKey.Name
			skKind = match.Table.SortKey.Kind
		}
		skPattern = match.Entity.SortKeyPattern
	}

	// Separate PK values from SK conditions
	pkFieldNames := make(map[string]bool, len(pkParams))
	for _, p := range pkParams {
		pkFieldNames[p.Name] = true
	}

	skFieldNames := make(map[string]bool, len(skParams))
	for _, p := range skParams {
		skFieldNames[p.Name] = true
	}

	pkValues := make(map[string]string)
	var skConditions []skCondition

	for _, arg := range kvArgs {
		field, cond, isPlain := parseSkCondition(arg)
		if cond == nil {
			return fmt.Errorf("invalid argument %q", arg)
		}
		if pkFieldNames[field] && isPlain {
			pkValues[field] = cond.value
		} else if skFieldNames[field] || !isPlain {
			skConditions = append(skConditions, *cond)
		} else {
			// Could be a PK field with non-equality op (error) or unknown field
			if pkFieldNames[field] {
				return fmt.Errorf("partition key field %q only supports equality (=)", field)
			}
			// Unknown field — maybe it's still a PK field not in the pattern,
			// or user made a typo. Try to be helpful.
			pkValues[field] = cond.value
		}
	}

	// Validate PK params
	if err := validateParams(pkParams, pkValues); err != nil {
		return fmt.Errorf("partition key: %w", err)
	}

	// Build the partition key value
	var pkPattern string
	if *gsiName != "" {
		for _, m := range match.Entity.GSIMappings {
			if strings.EqualFold(m.GSI, *gsiName) {
				pkPattern = m.PartitionPattern
				break
			}
		}
	} else {
		pkPattern = match.Entity.PartitionKeyPattern
	}

	pkValue, err := ddbcli.BuildKeyFromEntity(pkPattern, match.Entity, pkValues)
	if err != nil {
		return fmt.Errorf("building partition key: %w", err)
	}

	// Build the KeyConditionExpression
	exprNames := map[string]string{"#pk": pkAttrName}
	exprValues := map[string]types.AttributeValue{":pk": makeAttributeValue(pkValue, pkKind)}
	keyCondExpr := "#pk = :pk"

	if skAttrName != "" && len(skConditions) > 0 {
		exprNames["#sk"] = skAttrName
		skExpr, err := buildSKExpression(skConditions, skPattern, skKind, match.Entity, exprValues)
		if err != nil {
			return err
		}
		keyCondExpr += " AND " + skExpr
	} else if skAttrName != "" && len(skConditions) == 0 && skPattern != "" {
		// If SK pattern has no fields (pure literal), add equality condition
		prefix := ddbcli.LiteralPrefix(skPattern)
		if prefix == skPattern && prefix != "" {
			// Pure literal sort key — add exact match
			exprNames["#sk"] = skAttrName
			exprValues[":sk"] = makeAttributeValue(prefix, skKind)
			keyCondExpr += " AND #sk = :sk"
		}
	}

	// Build QueryInput
	input := &dynamodb.QueryInput{
		TableName:                 &match.Table.Name,
		KeyConditionExpression:    &keyCondExpr,
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprValues,
	}

	if indexName != "" {
		input.IndexName = &indexName
	}
	if *limit > 0 {
		input.Limit = aws.Int32(int32(*limit))
	}
	if *reverse {
		input.ScanIndexForward = aws.Bool(false)
	}
	if *consistent {
		if indexName != "" {
			return fmt.Errorf("--consistent is not supported on GSI queries")
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

	out, err := client.Query(ctx, input)
	if err != nil {
		return fmt.Errorf("Query: %w", err)
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

// buildSKExpression builds the sort key part of a KeyConditionExpression
// from parsed conditions.
func buildSKExpression(conditions []skCondition, skPattern, skKind string, entity schema.Entity, exprValues map[string]types.AttributeValue) (string, error) {
	fieldTypes := make(map[string]string)
	for _, f := range entity.Fields {
		fieldTypes[f.Tag] = f.Type
	}

	if len(conditions) == 1 {
		c := conditions[0]
		skVal, err := buildPartialSKValue(c.value, c.field, skPattern, entity)
		if err != nil {
			return "", err
		}

		switch c.op {
		case "=":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "#sk = :sk", nil
		case "^=":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "begins_with(#sk, :sk)", nil
		case ">":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "#sk > :sk", nil
		case ">=":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "#sk >= :sk", nil
		case "<":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "#sk < :sk", nil
		case "<=":
			exprValues[":sk"] = makeAttributeValue(skVal, skKind)
			return "#sk <= :sk", nil
		default:
			return "", fmt.Errorf("unsupported sort key operator %q", c.op)
		}
	}

	if len(conditions) == 2 {
		// Between: needs two conditions on the same field, one >= and one <=
		// (or > and < — we'll normalize to BETWEEN which is inclusive)
		c1, c2 := conditions[0], conditions[1]

		// Both must be on the same field
		if c1.field != c2.field {
			return "", fmt.Errorf("between requires two conditions on the same field, got %q and %q", c1.field, c2.field)
		}

		// Determine lo/hi
		var loVal, hiVal string
		var loErr, hiErr error

		// Normalize: find the lower and upper bound
		isLo := func(op string) bool { return op == ">=" || op == ">" }
		isHi := func(op string) bool { return op == "<=" || op == "<" }

		switch {
		case isLo(c1.op) && isHi(c2.op):
			loVal, loErr = buildPartialSKValue(c1.value, c1.field, skPattern, entity)
			hiVal, hiErr = buildPartialSKValue(c2.value, c2.field, skPattern, entity)
		case isHi(c1.op) && isLo(c2.op):
			loVal, loErr = buildPartialSKValue(c2.value, c2.field, skPattern, entity)
			hiVal, hiErr = buildPartialSKValue(c1.value, c1.field, skPattern, entity)
		default:
			return "", fmt.Errorf("between requires one lower bound (>= or >) and one upper bound (<= or <), got %q and %q", c1.op, c2.op)
		}

		if loErr != nil {
			return "", loErr
		}
		if hiErr != nil {
			return "", hiErr
		}

		exprValues[":sklo"] = makeAttributeValue(loVal, skKind)
		exprValues[":skhi"] = makeAttributeValue(hiVal, skKind)
		return "#sk BETWEEN :sklo AND :skhi", nil
	}

	return "", fmt.Errorf("at most 2 sort key conditions supported (for BETWEEN), got %d", len(conditions))
}

// buildPartialSKValue substitutes a single field value into the SK pattern,
// producing the full sort key value. For example, with pattern "ORDER#{orderID}"
// and value "order-1", it produces "ORDER#order-1".
func buildPartialSKValue(rawValue, fieldName, skPattern string, entity schema.Entity) (string, error) {
	values := map[string]string{fieldName: rawValue}

	// For patterns with a single field, we can build the full key.
	// For begins_with, we might want just the literal prefix + value.
	result, err := ddbcli.BuildKeyFromEntity(skPattern, entity, values)
	if err != nil {
		// If other fields are missing, it's because the pattern has multiple fields.
		// In that case, return what we can (literal prefix + formatted value).
		return "", fmt.Errorf("building sort key value: %w", err)
	}
	return result, nil
}

func findGSI(table schema.Table, name string) *schema.GSI {
	for i := range table.GSIs {
		if strings.EqualFold(table.GSIs[i].Name, name) {
			return &table.GSIs[i]
		}
	}
	return nil
}

func printQueryUsage() {
	fmt.Println(`ddb query - Query items by entity type and key conditions

Usage:
  ddb query <EntityType> <pk_field=value> [sk_conditions...] [flags]

Sort Key Operators:
  field=value       Exact match (equality)
  field^=prefix     Begins with
  field>=value      Greater than or equal
  field>value       Greater than
  field<=value      Less than or equal
  field<value       Less than
  Two conditions    BETWEEN (e.g. orderID>=A orderID<=Z)

Flags:
  --gsi NAME        Query a Global Secondary Index
  --limit N         Maximum number of items to return
  --reverse         Reverse sort key order (descending)
  --consistent      Use strongly consistent read (primary index only)
  --aws             Connect to AWS DynamoDB (default)
  --region STRING   AWS region
  --profile STRING  AWS profile name
  --endpoint URL    Custom DynamoDB endpoint
  --db PATH         Path to local database directory
  --memory          Use in-memory database

Examples:
  ddb query Order tenantID=tenant-42
  ddb query Order tenantID=tenant-42 orderID^=2024
  ddb query Order tenantID=tenant-42 orderID>=2024-01 orderID<=2024-12
  ddb query User --gsi GSI1 email=foo@bar.com
  ddb query Order tenantID=t1 --limit 10 --reverse`)
}
