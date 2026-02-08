package ddbsdk

import (
	"github.com/acksell/bezos/dynamodb/ddbstore"
	"github.com/acksell/bezos/dynamodb/table"
)

func NewMock(defs ...table.TableDefinition) IO {
	mock, err := ddbstore.New(ddbstore.StoreOptions{InMemory: true}, defs...)
	if err != nil {
		panic(err)
	}
	// todo implement projection expressions
	return New(mock)
}
