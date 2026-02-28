package ddbgen

// =============================================================================
// Loader output types (JSON-serialized from the loader sidecar)
// =============================================================================

// indexInfo holds the runtime data extracted from a PrimaryIndex variable by
// the loader sidecar. It combines the static metadata from Phase 1 (discover)
// with the runtime values obtained by executing user code.
type indexInfo struct {
	VarName      string      `json:"varName"`
	EntityType   string      `json:"entityType"`
	TableName    string      `json:"tableName"`
	PKDefName    string      `json:"pkDefName"`
	SKDefName    string      `json:"skDefName"`
	PartitionKey keyPattern  `json:"partitionKey"`
	SortKey      keyPattern  `json:"sortKey"`
	GSIs         []gsiInfo   `json:"gsis,omitempty"`
	IsVersioned  bool        `json:"isVersioned"`
	Fields       []fieldInfo `json:"fields"`
}

// keyPattern is a key's raw format string and DynamoDB attribute type.
type keyPattern struct {
	Pattern string `json:"pattern"`
	Kind    string `json:"kind"` // "S", "N", "B"
}

// gsiInfo holds runtime GSI data extracted from a SecondaryIndex.
type gsiInfo struct {
	Name      string     `json:"name"`
	Index     int        `json:"index"`
	PKDef     string     `json:"pkDef"`
	PKPattern keyPattern `json:"pkPattern"`
	SKDef     string     `json:"skDef"`
	SKPattern keyPattern `json:"skPattern"`
}

// fieldInfo mirrors discover.FieldInfo but with JSON tags for serialization.
type fieldInfo struct {
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
