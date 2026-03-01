package ddbgen

import "github.com/acksell/bezos/dynamodb/index/val"

// =============================================================================
// Index info types (used by code generation)
// =============================================================================

// IndexInfo holds the data extracted from a PrimaryIndex variable.
// Used by code generation to build type-safe accessor functions.
type IndexInfo struct {
	VarName      string      `json:"varName"`
	EntityType   string      `json:"entityType"`
	TableName    string      `json:"tableName"`
	PKDefName    string      `json:"pkDefName"`
	SKDefName    string      `json:"skDefName"`
	PartitionKey val.ValDef  `json:"partitionKey"`
	SortKey      *val.ValDef `json:"sortKey,omitempty"`
	GSIs         []GSIInfo   `json:"gsis,omitempty"`
	IsVersioned  bool        `json:"isVersioned"`
	Fields       []FieldInfo `json:"fields"`
}

// GSIInfo holds GSI data extracted from a SecondaryIndex.
type GSIInfo struct {
	Name      string      `json:"name"`
	Index     int         `json:"index"`
	PKDef     string      `json:"pkDef"`
	PKPattern val.ValDef  `json:"pkPattern"`
	SKDef     string      `json:"skDef"`
	SKPattern *val.ValDef `json:"skPattern,omitempty"`
}

// FieldInfo holds metadata about an entity struct field.
type FieldInfo struct {
	Name string `json:"name"`
	Tag  string `json:"tag"`
	Type string `json:"type"`
}

// =============================================================================
// Code generation types (template-ready data)
// =============================================================================

// indexData is the template-ready data for one index, transformed from indexInfo.
type indexData struct {
	Name         string
	IndexVarName string
	EntityType   string
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
	GSIs         []gsiData
	IsVersioned  bool
}

// HasEntity returns true if an entity type is associated with this index.
func (d indexData) HasEntity() bool { return d.EntityType != "" }

// keyData is the template-ready data for one key (partition or sort).
type keyData struct {
	Params           []paramData
	FormatExpr       string
	EntityFormatExpr string
	IsConstant       bool
	LiteralPrefix    string
	FieldRefNames    []string
	UsesFmt          bool
	UsesStrconv      bool
	UsesTime         bool
}

// paramData is one parameter in a key function signature.
type paramData struct {
	Name       string
	Type       string
	FieldType  string
	Formats    []string
	PrintfSpec string
}

// gsiData is the template-ready GSI data.
type gsiData struct {
	Name         string
	Index        int
	PartitionKey keyData
	SortKey      *keyData
	HasSortKey   bool
}
