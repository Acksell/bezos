package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/acksell/bezos/dynamodb/ddbui"
	"github.com/acksell/bezos/dynamodb/schema"
)

func runSchema() error {
	if len(os.Args) < 2 {
		printSchemaUsage()
		return fmt.Errorf("missing subcommand")
	}

	subcmd := os.Args[1]
	os.Args = append([]string{os.Args[0]}, os.Args[2:]...)

	schemas, err := loadSchemas()
	if err != nil {
		return err
	}

	switch subcmd {
	case "tables":
		return schemaListTables(schemas)
	case "entities":
		return schemaListEntities(schemas)
	case "describe":
		return schemaDescribe(schemas)
	case "help", "-h", "--help":
		printSchemaUsage()
		return nil
	default:
		printSchemaUsage()
		return fmt.Errorf("unknown subcommand %q", subcmd)
	}
}

func loadSchemas() ([]schema.Schema, error) {
	schemaFiles, err := DiscoverSchemas()
	if err != nil {
		return nil, fmt.Errorf("discovering schemas: %w", err)
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no schema files found\n\nGenerate schema files with:\n  //go:generate ddb gen\n  go generate ./...")
	}
	schemas, err := ddbui.LoadSchemaFilesRaw(schemaFiles)
	if err != nil {
		return nil, fmt.Errorf("loading schemas: %w", err)
	}
	return schemas, nil
}

func schemaListTables(schemas []schema.Schema) error {
	type tableInfo struct {
		Name         string `json:"name"`
		PartitionKey string `json:"partitionKey"`
		SortKey      string `json:"sortKey,omitempty"`
		GSICount     int    `json:"gsiCount"`
		EntityCount  int    `json:"entityCount"`
	}

	var tables []tableInfo
	for _, s := range schemas {
		for _, t := range s.Tables {
			info := tableInfo{
				Name:         t.Name,
				PartitionKey: t.PartitionKey.Name + " (" + t.PartitionKey.Kind + ")",
				GSICount:     len(t.GSIs),
				EntityCount:  len(t.Entities),
			}
			if t.SortKey != nil {
				info.SortKey = t.SortKey.Name + " (" + t.SortKey.Kind + ")"
			}
			tables = append(tables, info)
		}
	}
	return writeJSONStdout(tables)
}

func schemaListEntities(schemas []schema.Schema) error {
	// Check for --table flag
	var tableFilter string
	for i, arg := range os.Args[1:] {
		if arg == "--table" && i+1 < len(os.Args[1:])-1 {
			tableFilter = os.Args[i+2]
			break
		}
		if strings.HasPrefix(arg, "--table=") {
			tableFilter = strings.TrimPrefix(arg, "--table=")
			break
		}
	}

	type entityInfo struct {
		Type                string `json:"type"`
		Table               string `json:"table"`
		PartitionKeyPattern string `json:"partitionKeyPattern"`
		SortKeyPattern      string `json:"sortKeyPattern,omitempty"`
		FieldCount          int    `json:"fieldCount"`
		GSIMappingCount     int    `json:"gsiMappingCount"`
		IsVersioned         bool   `json:"isVersioned,omitempty"`
	}

	var entities []entityInfo
	for _, s := range schemas {
		for _, t := range s.Tables {
			if tableFilter != "" && !strings.EqualFold(t.Name, tableFilter) {
				continue
			}
			for _, e := range t.Entities {
				entities = append(entities, entityInfo{
					Type:                e.Type,
					Table:               t.Name,
					PartitionKeyPattern: e.PartitionKeyPattern,
					SortKeyPattern:      e.SortKeyPattern,
					FieldCount:          len(e.Fields),
					GSIMappingCount:     len(e.GSIMappings),
					IsVersioned:         e.IsVersioned,
				})
			}
		}
	}
	return writeJSONStdout(entities)
}

func schemaDescribe(schemas []schema.Schema) error {
	// Check for --table flag or positional entity name
	var tableFilter string
	var entityName string
	for i, arg := range os.Args[1:] {
		if arg == "--table" && i+1 < len(os.Args[1:])-1 {
			tableFilter = os.Args[i+2]
			break
		}
		if strings.HasPrefix(arg, "--table=") {
			tableFilter = strings.TrimPrefix(arg, "--table=")
			break
		}
	}
	if tableFilter == "" && len(os.Args) >= 2 {
		entityName = os.Args[1]
	}

	if tableFilter == "" && entityName == "" {
		return fmt.Errorf("provide an entity name or --table flag\n\nUsage:\n  ddb schema describe <EntityType>\n  ddb schema describe --table <tableName>")
	}

	// Describe a table
	if tableFilter != "" {
		return describeTable(schemas, tableFilter)
	}

	// Describe an entity
	return describeEntity(schemas, entityName)
}

func describeTable(schemas []schema.Schema, tableName string) error {
	for _, s := range schemas {
		for _, t := range s.Tables {
			if strings.EqualFold(t.Name, tableName) {
				return writeJSONStdout(t)
			}
		}
	}
	return fmt.Errorf("table %q not found", tableName)
}

type entityDescription struct {
	Type                string              `json:"type"`
	Table               string              `json:"table"`
	PartitionKeyPattern string              `json:"partitionKeyPattern"`
	SortKeyPattern      string              `json:"sortKeyPattern,omitempty"`
	Fields              []schema.Field      `json:"fields"`
	GSIMappings         []schema.GSIMapping `json:"gsiMappings,omitempty"`
	IsVersioned         bool                `json:"isVersioned,omitempty"`
	RequiredParams      []entityParam       `json:"requiredParams"`
}

type entityParam struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Source string `json:"source"` // "partitionKey", "sortKey"
}

func describeEntity(schemas []schema.Schema, entityName string) error {
	for _, s := range schemas {
		for _, t := range s.Tables {
			for _, e := range t.Entities {
				if strings.EqualFold(e.Type, entityName) {
					params := extractRequiredParams(e)
					desc := entityDescription{
						Type:                e.Type,
						Table:               t.Name,
						PartitionKeyPattern: e.PartitionKeyPattern,
						SortKeyPattern:      e.SortKeyPattern,
						Fields:              e.Fields,
						GSIMappings:         e.GSIMappings,
						IsVersioned:         e.IsVersioned,
						RequiredParams:      params,
					}
					return writeJSONStdout(desc)
				}
			}
		}
	}
	return fmt.Errorf("entity %q not found", entityName)
}

// extractRequiredParams parses key patterns to determine what parameters
// the user needs to provide for get/query operations.
func extractRequiredParams(e schema.Entity) []entityParam {
	fieldTypes := make(map[string]string)
	for _, f := range e.Fields {
		fieldTypes[f.Tag] = f.Type
	}

	var params []entityParam
	params = append(params, paramsFromPattern(e.PartitionKeyPattern, fieldTypes, "partitionKey")...)
	params = append(params, paramsFromPattern(e.SortKeyPattern, fieldTypes, "sortKey")...)
	return params
}

func paramsFromPattern(pattern string, fieldTypes map[string]string, source string) []entityParam {
	if pattern == "" {
		return nil
	}
	var params []entityParam
	// Simple extraction of {field} references
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '{' {
			end := strings.Index(pattern[i:], "}")
			if end < 0 {
				break
			}
			ref := pattern[i+1 : i+end]
			// Parse field:format:spec
			parts := strings.Split(ref, ":")
			fieldName := parts[0]
			typ := fieldTypes[fieldName]
			if typ == "" {
				typ = "string"
			}
			params = append(params, entityParam{
				Name:   fieldName,
				Type:   typ,
				Source: source,
			})
			i += end
		}
	}
	return params
}

func writeJSONStdout(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printSchemaUsage() {
	fmt.Println(`ddb schema - Inspect DynamoDB schema definitions

Usage:
  ddb schema <subcommand> [flags]

Subcommands:
  tables                    List all tables
  entities [--table NAME]   List all entities (optionally filtered by table)
  describe <EntityType>     Describe an entity type (fields, keys, GSIs)
  describe --table <NAME>   Describe a table (keys, GSIs, all entities)

Examples:
  ddb schema tables
  ddb schema entities
  ddb schema entities --table users
  ddb schema describe User
  ddb schema describe --table orders`)
}
