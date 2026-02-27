package ddbgen

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// SchemaFile is the root structure for the schema_dynamodb.yaml file.
type SchemaFile struct {
	Tables []SchemaOutput `yaml:"tables"`
}

// SchemaOutput represents the YAML schema for a single table.
type SchemaOutput struct {
	Table    TableSchema    `yaml:"table"`
	Entities []EntitySchema `yaml:"entities"`
}

// TableSchema describes a DynamoDB table structure.
type TableSchema struct {
	Name         string      `yaml:"name"`
	PartitionKey KeyDefYAML  `yaml:"partitionKey"`
	SortKey      *KeyDefYAML `yaml:"sortKey,omitempty"`
	GSIs         []GSISchema `yaml:"gsis,omitempty"`
}

// KeyDefYAML is a key definition for YAML output.
type KeyDefYAML struct {
	Name string `yaml:"name"`
	Kind string `yaml:"kind"` // "S", "N", or "B"
}

// GSISchema describes a Global Secondary Index.
type GSISchema struct {
	Name         string      `yaml:"name"`
	PartitionKey KeyDefYAML  `yaml:"partitionKey"`
	SortKey      *KeyDefYAML `yaml:"sortKey,omitempty"`
}

// EntitySchema describes an entity type stored in a table.
type EntitySchema struct {
	Type                string           `yaml:"type"`
	PartitionKeyPattern string           `yaml:"partitionKeyPattern"`
	SortKeyPattern      string           `yaml:"sortKeyPattern,omitempty"`
	Fields              []FieldSchema    `yaml:"fields"`
	GSIMappings         []GSIMappingYAML `yaml:"gsiMappings,omitempty"`
	IsVersioned         bool             `yaml:"isVersioned,omitempty"`
}

// FieldSchema describes an entity field.
type FieldSchema struct {
	Name string `yaml:"name"`
	Tag  string `yaml:"tag"`
	Type string `yaml:"type"`
}

// GSIMappingYAML describes how an entity maps to a GSI.
type GSIMappingYAML struct {
	GSI              string `yaml:"gsi"`
	PartitionPattern string `yaml:"partitionPattern"`
	SortPattern      string `yaml:"sortPattern,omitempty"`
}

// GenerateSchemas converts a DiscoverResult into per-table SchemaOutputs.
// Returns a slice of SchemaOutput, one per table.
func GenerateSchemas(result *DiscoverResult) ([]SchemaOutput, error) {
	if len(result.Indexes) == 0 {
		return nil, fmt.Errorf("no indexes to generate schemas for")
	}

	// Group indexes by table name
	tableIndexes := make(map[string][]IndexInfo)
	tableSchemas := make(map[string]*SchemaOutput)

	for _, idx := range result.Indexes {
		tableName := idx.TableName
		if tableName == "" {
			// Fallback: use entity type as table identifier
			tableName = idx.EntityType
		}
		tableIndexes[tableName] = append(tableIndexes[tableName], idx)
	}

	for tableName, indexes := range tableIndexes {
		// Use the first index to get table structure (all should share same table)
		firstIdx := indexes[0]

		schema := &SchemaOutput{
			Table: TableSchema{
				Name: tableName,
				PartitionKey: KeyDefYAML{
					Name: firstIdx.PKDefName,
					Kind: string(firstIdx.PartitionKey.Kind),
				},
			},
			Entities: make([]EntitySchema, 0, len(indexes)),
		}

		// Add sort key if present
		if firstIdx.SortKey.Pattern != "" {
			schema.Table.SortKey = &KeyDefYAML{
				Name: firstIdx.SKDefName,
				Kind: string(firstIdx.SortKey.Kind),
			}
		}

		// Add GSIs from the first index (table structure)
		for _, gsi := range firstIdx.GSIs {
			gsiSchema := GSISchema{
				Name: gsi.Name,
				PartitionKey: KeyDefYAML{
					Name: gsi.PKDef,
					Kind: string(gsi.PKPattern.Kind),
				},
			}
			if gsi.SKPattern.Pattern != "" {
				gsiSchema.SortKey = &KeyDefYAML{
					Name: gsi.SKDef,
					Kind: string(gsi.SKPattern.Kind),
				}
			}
			schema.Table.GSIs = append(schema.Table.GSIs, gsiSchema)
		}

		// Add entity schemas
		for _, idx := range indexes {
			entity := EntitySchema{
				Type:                idx.EntityType,
				PartitionKeyPattern: idx.PartitionKey.Pattern,
				IsVersioned:         idx.IsVersioned,
			}

			if idx.SortKey.Pattern != "" {
				entity.SortKeyPattern = idx.SortKey.Pattern
			}

			// Add fields from discovery result
			if fields, ok := result.EntityFields[idx.EntityType]; ok {
				for _, f := range fields {
					entity.Fields = append(entity.Fields, FieldSchema{
						Name: f.Name,
						Tag:  f.Tag,
						Type: f.Type,
					})
				}
			}

			// Add GSI mappings
			for _, gsi := range idx.GSIs {
				mapping := GSIMappingYAML{
					GSI:              gsi.Name,
					PartitionPattern: gsi.PKPattern.Pattern,
				}
				if gsi.SKPattern.Pattern != "" {
					mapping.SortPattern = gsi.SKPattern.Pattern
				}
				entity.GSIMappings = append(entity.GSIMappings, mapping)
			}

			schema.Entities = append(schema.Entities, entity)
		}

		tableSchemas[tableName] = schema
	}

	// Convert map to slice
	var schemas []SchemaOutput
	for _, schema := range tableSchemas {
		schemas = append(schemas, *schema)
	}

	return schemas, nil
}

// WriteSchemas writes all schemas to a single schema_dynamodb.yaml file.
func WriteSchemas(schemas []SchemaOutput, dir string) error {
	path := filepath.Join(dir, "schema_dynamodb.yaml")

	file := SchemaFile{Tables: schemas}

	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(1)
	if err := encoder.Encode(file); err != nil {
		return fmt.Errorf("marshaling schemas: %w", err)
	}
	encoder.Close()

	// Add header comment
	header := []byte("# Generated by ddbgen. DO NOT EDIT.\n\n")
	data := append(header, buf.Bytes()...)

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing schema file: %w", err)
	}
	return nil
}
