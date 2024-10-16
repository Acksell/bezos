package normddb

import (
	"context"
)

func NewMock(defs ...TableDefinition) *dynamock {
	return &dynamock{
		store: newMockStore(defs...),
	}
}

type dynamock struct {
	store *mockStore
}

type IO interface {
	Writer
	Reader
}

var _ Writer = &dynamock{}
var _ Reader = &dynamock{}

type Writer interface {
	NewTx(...TxOption) Txer
	NewBatch(...BatchOption) Batcher
}

type Txer interface {
	AddAction(context.Context, Action) error
	Commit(context.Context) error
}

type Batcher interface {
	AddAction(context.Context, Action) error
	Write(context.Context) error
}

type Reader interface {
	NewQuery(TableDefinition, KeyCondition, ...QueryOption) Querier
	NewGet(...GetOption) Getter
}

type Querier interface {
	Next(context.Context) (QueryResult, error)
	QueryAll(context.Context) (QueryResult, error)
}

type Getter interface {
	Lookup(context.Context, ItemIdentifier) (DynamoEntity, error)
	TxLookupMany(context.Context, ...ItemIdentifier) ([]DynamoEntity, error)
	BatchLookupMany(context.Context, ...ItemIdentifier) ([]DynamoEntity, error)
}
