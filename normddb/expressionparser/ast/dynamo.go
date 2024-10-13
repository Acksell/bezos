package ast

// func getAttrVal(name string, map[string]AttributeValue)  (AttributeValue, bool) {
// 	return map[name], ok
// }

// type Document interface {
// 	// Get the value of the attribute at the given path
// 	GetAttrVal(name string) (AttributeValue, bool)

// 	// Check if the document has the attribute at the given path
// 	HasAttribute(name string) bool

// 	// Get the table definition for the document
// 	GetTableDef() Table // ? pass as separate argument to eval instead of method on document?

// 	// Get the primary key for the document
// 	GetPrimaryKey() map[string]interface{} // ? maybe just use dynamodb attribute value types directly?
// }

// type Index interface {
// 	GetPartitionKeyName() string
// 	GetPartitionKeyType() string
// 	GetSortKeyName() string
// 	GetSortKeyType() string
// }

// type Table interface {
// 	Primary() Index
// 	GetGSIs() []GSI
// }

// type GSI interface {
// 	Index
// }
