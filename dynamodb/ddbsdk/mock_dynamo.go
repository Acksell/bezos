package bzoddb

import (
	"bezos/dynamodb/ddbstore"
	"bezos/dynamodb/table"
)

func NewMock(defs ...table.TableDefinition) IO {
	mock := ddbstore.NewStore(defs...)
	// works if mockddb.NewStore() is a good enough mock of AWSDynamoClientV2 iface
	// todo implement projection expressions
	return New(mock)
}

// todo can remove if mockddb.NewStore() supports all required DDB features.
type dynamock struct {
	client AWSDynamoClientV2
}

var _ IO = &dynamock{}
var _ Writer = &dynamock{}
var _ Reader = &dynamock{}
