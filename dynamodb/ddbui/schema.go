package ddbui

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/acksell/bezos/dynamodb/table"
	"gopkg.in/yaml.v3"
)

// SchemaFile represents the YAML schema for a single table.
type SchemaFile struct {
	Table    TableSchema    `yaml:"table" json:"table"`
	Entities []EntitySchema `yaml:"entities" json:"entities"`
}

// TableSchema describes a DynamoDB table structure.
type TableSchema struct {
	Name         string      `yaml:"name" json:"name"`
	PartitionKey KeyDefYAML  `yaml:"partitionKey" json:"partitionKey"`
	SortKey      *KeyDefYAML `yaml:"sortKey,omitempty" json:"sortKey,omitempty"`
	GSIs         []GSISchema `yaml:"gsis,omitempty" json:"gsis,omitempty"`
}

// KeyDefYAML is a key definition for YAML.
type KeyDefYAML struct {
	Name string `yaml:"name" json:"name"`
	Kind string `yaml:"kind" json:"kind"` // "S", "N", or "B"
}

// GSISchema describes a Global Secondary Index.
type GSISchema struct {
	Name         string      `yaml:"name" json:"name"`
	PartitionKey KeyDefYAML  `yaml:"partitionKey" json:"partitionKey"`
	SortKey      *KeyDefYAML `yaml:"sortKey,omitempty" json:"sortKey,omitempty"`
}

// EntitySchema describes an entity type stored in a table.
type EntitySchema struct {
	Type                string           `yaml:"type" json:"type"`
	PartitionKeyPattern string           `yaml:"partitionKeyPattern" json:"partitionKeyPattern"`
	SortKeyPattern      string           `yaml:"sortKeyPattern,omitempty" json:"sortKeyPattern,omitempty"`
	Fields              []FieldSchema    `yaml:"fields" json:"fields"`
	GSIMappings         []GSIMappingYAML `yaml:"gsiMappings,omitempty" json:"gsiMappings,omitempty"`
	IsVersioned         bool             `yaml:"isVersioned,omitempty" json:"isVersioned,omitempty"`
}

// FieldSchema describes an entity field.
type FieldSchema struct {
	Name string `yaml:"name" json:"name"`
	Tag  string `yaml:"tag" json:"tag"`
	Type string `yaml:"type" json:"type"`
}

// GSIMappingYAML describes how an entity maps to a GSI.
type GSIMappingYAML struct {
	GSI              string `yaml:"gsi" json:"gsi"`
	PartitionPattern string `yaml:"partitionPattern" json:"partitionPattern"`
	SortPattern      string `yaml:"sortPattern,omitempty" json:"sortPattern,omitempty"`
}

// LoadedSchema contains all loaded schema information.
type LoadedSchema struct {
	// Tables maps table name to schema file
	Tables map[string]*SchemaFile
	// TableDefinitions are the runtime table definitions for ddbstore
	TableDefinitions []table.TableDefinition
}

// LoadSchemas loads schema files matching the given glob pattern.
func LoadSchemas(pattern string) (*LoadedSchema, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob pattern error: %w", err)
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no schema files found matching: %s", pattern)
	}

	schema := &LoadedSchema{
		Tables: make(map[string]*SchemaFile),
	}

	for _, path := range matches {
		sf, err := loadSchemaFile(path)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}

		if existing, ok := schema.Tables[sf.Table.Name]; ok {
			// Merge entities into existing table schema
			existing.Entities = append(existing.Entities, sf.Entities...)
		} else {
			schema.Tables[sf.Table.Name] = sf
		}
	}

	// Convert to table definitions
	for _, sf := range schema.Tables {
		def := toTableDefinition(sf)
		schema.TableDefinitions = append(schema.TableDefinitions, def)
	}

	return schema, nil
}

// loadSchemaFile reads and parses a single schema YAML file.
func loadSchemaFile(path string) (*SchemaFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var sf SchemaFile
	if err := yaml.Unmarshal(data, &sf); err != nil {
		return nil, err
	}

	if sf.Table.Name == "" {
		return nil, fmt.Errorf("table name is required")
	}

	return &sf, nil
}

// toTableDefinition converts a SchemaFile to a runtime TableDefinition.
func toTableDefinition(sf *SchemaFile) table.TableDefinition {
	def := table.TableDefinition{
		Name: sf.Table.Name,
		KeyDefinitions: table.PrimaryKeyDefinition{
			PartitionKey: table.KeyDef{
				Name: sf.Table.PartitionKey.Name,
				Kind: toKeyKind(sf.Table.PartitionKey.Kind),
			},
		},
	}

	if sf.Table.SortKey != nil {
		def.KeyDefinitions.SortKey = table.KeyDef{
			Name: sf.Table.SortKey.Name,
			Kind: toKeyKind(sf.Table.SortKey.Kind),
		}
	}

	for _, gsi := range sf.Table.GSIs {
		gsiDef := table.GSIDefinition{
			Name: gsi.Name,
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{
					Name: gsi.PartitionKey.Name,
					Kind: toKeyKind(gsi.PartitionKey.Kind),
				},
			},
		}
		if gsi.SortKey != nil {
			gsiDef.KeyDefinitions.SortKey = table.KeyDef{
				Name: gsi.SortKey.Name,
				Kind: toKeyKind(gsi.SortKey.Kind),
			}
		}
		def.GSIs = append(def.GSIs, gsiDef)
	}

	return def
}

// toKeyKind converts a string kind to table.KeyKind.
func toKeyKind(kind string) table.KeyKind {
	switch kind {
	case "S":
		return table.KeyKindS
	case "N":
		return table.KeyKindN
	case "B":
		return table.KeyKindB
	default:
		return table.KeyKindS
	}
}
