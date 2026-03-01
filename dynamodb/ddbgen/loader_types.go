package ddbgen

import "github.com/acksell/bezos/dynamodb/index/val"

// =============================================================================
// Index info types (used by code generation)
// =============================================================================

// indexInfo holds the data extracted from a PrimaryIndex variable.
// Used by code generation to build type-safe accessor functions.
type indexInfo struct {
	VarName      string
	EntityType   string
	TableName    string
	PKDefName    string
	SKDefName    string
	PartitionKey val.ValDef
	SortKey      *val.ValDef
	GSIs         []gsiInfo
	IsVersioned  bool
	Fields       []fieldInfo
}

// gsiInfo holds GSI data extracted from a SecondaryIndex.
type gsiInfo struct {
	Name      string
	Index     int
	PKDef     string
	PKPattern val.ValDef
	SKDef     string
	SKPattern *val.ValDef
}

// fieldInfo holds metadata about an entity struct field.
type fieldInfo struct {
	Name string
	Tag  string
	Type string
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
