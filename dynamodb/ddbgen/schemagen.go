package ddbgen

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	"github.com/acksell/bezos/dynamodb/index/val"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Schema types
// =============================================================================

type schemaTable struct {
	Name         string         `yaml:"name"`
	PartitionKey schemaKeyDef   `yaml:"partitionKey"`
	SortKey      *schemaKeyDef  `yaml:"sortKey,omitempty"`
	GSIs         []schemaGSI    `yaml:"gsis,omitempty"`
	Entities     []schemaEntity `yaml:"entities,omitempty"`
}

type schemaKeyDef struct {
	Name string `yaml:"name"`
	Kind string `yaml:"kind"`
}

type schemaGSI struct {
	Name         string        `yaml:"name"`
	PartitionKey schemaKeyDef  `yaml:"partitionKey"`
	SortKey      *schemaKeyDef `yaml:"sortKey,omitempty"`
}

type schemaEntity struct {
	Type                string         `yaml:"type"`
	PartitionKeyPattern string         `yaml:"partitionKeyPattern"`
	SortKeyPattern      string         `yaml:"sortKeyPattern,omitempty"`
	Fields              []schemaField  `yaml:"fields"`
	GSIMappings         []schemaGSIMap `yaml:"gsiMappings,omitempty"`
	IsVersioned         bool           `yaml:"isVersioned,omitempty"`
}

type schemaField struct {
	Name string `yaml:"name"`
	Tag  string `yaml:"tag"`
	Type string `yaml:"type"`
}

type schemaGSIMap struct {
	GSI              string `yaml:"gsi"`
	PartitionPattern string `yaml:"partitionPattern"`
	SortPattern      string `yaml:"sortPattern,omitempty"`
}

type schemaRoot struct {
	Tables []schemaTable `yaml:"tables"`
}

// =============================================================================
// ValDef helpers
// =============================================================================

// valDefPattern extracts a pattern string from a ValDef for schema display.
func valDefPattern(vd val.ValDef) string {
	if vd.Format != nil {
		return vd.Format.Raw
	}
	if vd.FromField != "" {
		return "{" + vd.FromField + "}"
	}
	if vd.Const != nil {
		if vd.Const.Kind == val.SpecKindB {
			if b, ok := vd.Const.Value.([]byte); ok {
				return base64.StdEncoding.EncodeToString(b)
			}
		}
		return fmt.Sprintf("%v", vd.Const.Value)
	}
	return ""
}

// valDefKind extracts the DynamoDB attribute type from a ValDef.
func valDefKind(vd val.ValDef) string {
	if vd.Format != nil {
		return string(vd.Format.Kind)
	}
	if vd.Const != nil {
		return string(vd.Const.Kind)
	}
	return "S" // Default for FromField
}

// =============================================================================
// Schema generation
// =============================================================================

func generateSchemaFiles(schemaDir string, indexes []indexInfo) error {
	// Group indexes by table, preserving discovery order
	tableIndexes := make(map[string][]indexInfo)
	var tableOrder []string
	for _, idx := range indexes {
		tableName := idx.TableName
		if tableName == "" {
			tableName = idx.EntityType
		}
		if _, exists := tableIndexes[tableName]; !exists {
			tableOrder = append(tableOrder, tableName)
		}
		tableIndexes[tableName] = append(tableIndexes[tableName], idx)
	}

	var tables []schemaTable
	for _, tableName := range tableOrder {
		idxs := tableIndexes[tableName]
		firstIdx := idxs[0]
		tbl := schemaTable{
			Name:         tableName,
			PartitionKey: schemaKeyDef{Name: firstIdx.PKDefName, Kind: valDefKind(firstIdx.PartitionKey)},
		}
		if firstIdx.SortKey != nil && !firstIdx.SortKey.IsZero() {
			tbl.SortKey = &schemaKeyDef{Name: firstIdx.SKDefName, Kind: valDefKind(*firstIdx.SortKey)}
		}
		for _, gsi := range firstIdx.GSIs {
			g := schemaGSI{
				Name:         gsi.Name,
				PartitionKey: schemaKeyDef{Name: gsi.PKDef, Kind: valDefKind(gsi.PKPattern)},
			}
			if gsi.SKPattern != nil && !gsi.SKPattern.IsZero() {
				g.SortKey = &schemaKeyDef{Name: gsi.SKDef, Kind: valDefKind(*gsi.SKPattern)}
			}
			tbl.GSIs = append(tbl.GSIs, g)
		}
		for _, idx := range idxs {
			entity := schemaEntity{
				Type:                idx.EntityType,
				PartitionKeyPattern: valDefPattern(idx.PartitionKey),
				IsVersioned:         idx.IsVersioned,
			}
			if idx.SortKey != nil && !idx.SortKey.IsZero() {
				entity.SortKeyPattern = valDefPattern(*idx.SortKey)
			}
			for _, f := range idx.Fields {
				entity.Fields = append(entity.Fields, schemaField{Name: f.Name, Tag: f.Tag, Type: f.Type})
			}
			for _, gsi := range idx.GSIs {
				mapping := schemaGSIMap{GSI: gsi.Name, PartitionPattern: valDefPattern(gsi.PKPattern)}
				if gsi.SKPattern != nil && !gsi.SKPattern.IsZero() {
					mapping.SortPattern = valDefPattern(*gsi.SKPattern)
				}
				entity.GSIMappings = append(entity.GSIMappings, mapping)
			}
			tbl.Entities = append(tbl.Entities, entity)
		}
		tables = append(tables, tbl)
	}

	schema := schemaRoot{Tables: tables}

	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(1)
	if err := encoder.Encode(schema); err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	encoder.Close()

	header := []byte("# Generated by ddbgen. DO NOT EDIT.\n\n")
	data := append(header, buf.Bytes()...)

	yamlPath := filepath.Join(schemaDir, "schema_dynamodb.yaml")
	if err := os.WriteFile(yamlPath, data, 0644); err != nil {
		return fmt.Errorf("writing schema file: %w", err)
	}
	fmt.Printf("ddb gen: generated %s (%d tables)\n", yamlPath, len(tables))

	schemaGoCode := `// Code generated by ddbgen. DO NOT EDIT.

package schema

import (
	_ "embed"

	"github.com/acksell/bezos/dynamodb/schema"
	"gopkg.in/yaml.v3"
)

//go:embed schema_dynamodb.yaml
var schemaYAML []byte

// Schema contains the DynamoDB table and entity definitions for this package.
// Pass this to ddbui.NewServer to enable schema-aware debugging UI.
var Schema schema.Schema

func init() {
	if err := yaml.Unmarshal(schemaYAML, &Schema); err != nil {
		panic("ddbgen: failed to parse embedded schema: " + err.Error())
	}
}
`

	schemaGoPath := filepath.Join(schemaDir, "schema_gen.go")
	if err := os.WriteFile(schemaGoPath, []byte(schemaGoCode), 0644); err != nil {
		return fmt.Errorf("writing schema go file: %w", err)
	}
	fmt.Printf("ddb gen: generated %s\n", schemaGoPath)

	return nil
}
