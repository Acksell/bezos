package ddbui

import (
	"fmt"
	"os"

	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/schema"
	"github.com/acksell/bezos/dynamodb/table"
	"gopkg.in/yaml.v3"
)

// LoadedSchema contains all loaded schema information with parsed patterns.
// This is the internal representation used by ddbui, enriched with pattern
// parsing information that isn't part of the raw schema.Schema.
type LoadedSchema struct {
	// Tables maps table name to enriched table info
	Tables map[string]*EnrichedTable
	// TableDefinitions are the runtime table definitions for ddbstore
	TableDefinitions []table.TableDefinition
}

// EnrichedTable wraps a schema.Table with parsed pattern information.
type EnrichedTable struct {
	schema.Table
	// EnrichedEntities have parsed patterns for UI key building
	EnrichedEntities []EnrichedEntity
}

// EnrichedEntity wraps a schema.Entity with parsed pattern information.
type EnrichedEntity struct {
	schema.Entity
	// Parsed pattern info for UI - computed at load time
	PartitionKeyParsed *ParsedPattern `json:"partitionKeyParsed,omitempty"`
	SortKeyParsed      *ParsedPattern `json:"sortKeyParsed,omitempty"`
}

// ParsedPattern represents a parsed key pattern with its parts.
// This allows the UI to build keys without parsing patterns itself.
type ParsedPattern struct {
	Parts []PatternPart `json:"parts"`
}

// PatternPart is either a literal string or a variable reference.
type PatternPart struct {
	IsLiteral  bool     `json:"isLiteral"`
	Value      string   `json:"value"`                // literal value or variable name
	Formats    []string `json:"formats,omitempty"`    // format modifiers for variables
	PrintfSpec string   `json:"printfSpec,omitempty"` // printf spec for variables
	FieldType  string   `json:"fieldType,omitempty"`  // Go type of the field (from entity fields)
}

// LoadFromSchema creates a LoadedSchema from one or more schema.Schema objects.
// This is the primary constructor for embedded use cases.
func LoadFromSchema(schemas ...schema.Schema) (*LoadedSchema, error) {
	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas provided")
	}

	loaded := &LoadedSchema{
		Tables: make(map[string]*EnrichedTable),
	}

	for _, s := range schemas {
		for _, t := range s.Tables {
			if existing, ok := loaded.Tables[t.Name]; ok {
				// Merge entities into existing table
				for _, entity := range t.Entities {
					enriched := enrichEntity(entity)
					existing.EnrichedEntities = append(existing.EnrichedEntities, enriched)
					existing.Entities = append(existing.Entities, entity)
				}
			} else {
				enrichedTable := &EnrichedTable{
					Table:            t,
					EnrichedEntities: make([]EnrichedEntity, 0, len(t.Entities)),
				}
				for _, entity := range t.Entities {
					enrichedTable.EnrichedEntities = append(enrichedTable.EnrichedEntities, enrichEntity(entity))
				}
				loaded.Tables[t.Name] = enrichedTable
			}
		}
	}

	// Build table definitions
	for _, t := range loaded.Tables {
		loaded.TableDefinitions = append(loaded.TableDefinitions, toTableDefinition(&t.Table))
	}

	return loaded, nil
}

// LoadSchemaFiles loads schema from YAML file paths.
// This is used by the standalone CLI.
func LoadSchemaFiles(files []string) (*LoadedSchema, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no schema files provided")
	}

	var schemas []schema.Schema
	for _, path := range files {
		s, err := loadSchemaFile(path)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
		schemas = append(schemas, s)
	}

	return LoadFromSchema(schemas...)
}

// LoadSchemaFilesRaw loads schema files and returns the raw schema.Schema objects.
// This is used by the CLI to pass schemas to NewServer.
func LoadSchemaFilesRaw(files []string) ([]schema.Schema, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no schema files provided")
	}

	var schemas []schema.Schema
	for _, path := range files {
		s, err := loadSchemaFile(path)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
		schemas = append(schemas, s)
	}

	return schemas, nil
}

// TableDefinitionsFromSchemas extracts table.TableDefinition objects from schemas.
// This is useful for creating a ddbstore.Store with the correct table definitions.
func TableDefinitionsFromSchemas(schemas ...schema.Schema) []table.TableDefinition {
	seen := make(map[string]bool)
	var defs []table.TableDefinition
	for _, s := range schemas {
		for _, t := range s.Tables {
			if seen[t.Name] {
				continue
			}
			seen[t.Name] = true
			defs = append(defs, toTableDefinition(&t))
		}
	}
	return defs
}

// loadSchemaFile reads and parses a schema_dynamodb.yaml file.
func loadSchemaFile(path string) (schema.Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return schema.Schema{}, err
	}

	var s schema.Schema
	if err := yaml.Unmarshal(data, &s); err != nil {
		return schema.Schema{}, err
	}

	if len(s.Tables) == 0 {
		return schema.Schema{}, fmt.Errorf("no tables defined in schema file")
	}

	// Validate each table
	for i, t := range s.Tables {
		if t.Name == "" {
			return schema.Schema{}, fmt.Errorf("table[%d]: name is required", i)
		}
	}

	return s, nil
}

// enrichEntity parses the key patterns for an entity.
func enrichEntity(entity schema.Entity) EnrichedEntity {
	enriched := EnrichedEntity{Entity: entity}

	// Build a map of field tag -> field type for resolving variable types
	fieldTypes := make(map[string]string)
	for _, f := range entity.Fields {
		fieldTypes[f.Tag] = f.Type
	}

	if entity.PartitionKeyPattern != "" {
		enriched.PartitionKeyParsed = parsePattern(entity.PartitionKeyPattern, fieldTypes)
	}
	if entity.SortKeyPattern != "" {
		enriched.SortKeyParsed = parsePattern(entity.SortKeyPattern, fieldTypes)
	}

	return enriched
}

// toTableDefinition converts a schema.Table to a runtime TableDefinition.
func toTableDefinition(t *schema.Table) table.TableDefinition {
	def := table.TableDefinition{
		Name: t.Name,
		KeyDefinitions: table.PrimaryKeyDefinition{
			PartitionKey: table.KeyDef{
				Name: t.PartitionKey.Name,
				Kind: toKeyKind(t.PartitionKey.Kind),
			},
		},
	}

	if t.SortKey != nil {
		def.KeyDefinitions.SortKey = table.KeyDef{
			Name: t.SortKey.Name,
			Kind: toKeyKind(t.SortKey.Kind),
		}
	}

	for _, gsi := range t.GSIs {
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

// parsePattern parses a key pattern string into a ParsedPattern using the val package.
func parsePattern(pattern string, fieldTypes map[string]string) *ParsedPattern {
	spec, err := val.ParseFmt(pattern)
	if err != nil {
		// If parsing fails, return a single literal part with the whole pattern
		return &ParsedPattern{
			Parts: []PatternPart{{IsLiteral: true, Value: pattern}},
		}
	}

	parts := make([]PatternPart, len(spec.Parts))
	for i, p := range spec.Parts {
		parts[i] = PatternPart{
			IsLiteral:  p.IsLiteral,
			Value:      p.Value,
			Formats:    p.Formats,
			PrintfSpec: p.PrintfSpec,
		}
		// For variables, look up the field type
		if !p.IsLiteral {
			// The value might be a dotted path like "user.id", use just the field name
			fieldName := p.Value
			if ft, ok := fieldTypes[fieldName]; ok {
				parts[i].FieldType = ft
			}
		}
	}

	return &ParsedPattern{Parts: parts}
}
