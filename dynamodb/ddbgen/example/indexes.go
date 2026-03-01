// Package example demonstrates how to use ddbgen with index definitions.
//
// Define indexes using indices.Add to register PrimaryIndex definitions,
// then run go generate to create gen/main.go and generate code.
package example

//go:generate ddb gen

import (
	"fmt"
	"time"

	"github.com/acksell/bezos/dynamodb/index"
	"github.com/acksell/bezos/dynamodb/index/indices"
	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

var UserTable = table.TableDefinition{
	Name: "users",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	GSIs: []table.GSIDefinition{
		{
			Name: "GSI1",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

func init() {
	indices.Add(index.PrimaryIndex[User]{
		Table:        UserTable,
		PartitionKey: val.Fmt("USER#{id}"),
		SortKey:      val.Fmt("PROFILE").Ptr(),
		Secondary: []index.SecondaryIndex{
			{
				GSI:       UserTable.GSIs[0],
				Partition: val.Fmt("EMAIL#{email}"),
				Sort:      val.Fmt("USER#{id}").Ptr(),
			},
		},
	})
}

var OrderTable = table.TableDefinition{
	Name: "orders",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

var _ = indices.Add(index.PrimaryIndex[Order]{
	Table:        OrderTable,
	PartitionKey: val.Fmt("TENANT#{tenantID}"),
	SortKey:      val.Fmt("ORDER#{orderID}").Ptr(),
})

// MessageTable demonstrates using int64 fields in keys
var MessageTable = table.TableDefinition{
	Name: "messages",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

// _ = indices.Add demonstrates int64 in sort key with warning
var _ = indices.Add(index.PrimaryIndex[Message]{
	Table:        MessageTable,
	PartitionKey: val.Fmt("CHAT#{chatID}"),
	SortKey:      val.Fmt("MSG#{sequenceNum}").Ptr(), // int64 without padding - should warn
})

// EventTable demonstrates time.Time keys
var EventTable = table.TableDefinition{
	Name: "events",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
}

// Event demonstrates time.Time key formatting
type Event struct {
	EventID   string    `dynamodbav:"eventID"`
	Timestamp time.Time `dynamodbav:"timestamp"`
	EventType string    `dynamodbav:"eventType"`
}

func (e *Event) IsValid() error { return nil }

// _ = indices.Add demonstrates time.Time in sort key with unixnano format (padded)
var _ = indices.Add(index.PrimaryIndex[Event]{
	Table:        EventTable,
	PartitionKey: val.Fmt("EVENT#{eventID}"),
	SortKey:      val.Fmt("EVENT#{timestamp:unixnano:%020d}").Ptr(), // padded - no warning
})

var SingleTable = table.TableDefinition{
	Name: "single-table",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
		SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
	},
	GSIs: []table.GSIDefinition{
		{
			Name: "GSI1",
			KeyDefinitions: table.PrimaryKeyDefinition{
				PartitionKey: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
				SortKey:      table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
			},
		},
	},
}

type RandomEntity struct {
	ID string `dynamodbav:"id"`
}

func (e RandomEntity) IsValid() error {
	if e.ID == "" {
		return fmt.Errorf("ID cannot be empty")
	}
	return nil
}

// TODO support index.PrimaryIndex[User] on SingleTable - i.e. support an entity on different tables, useful for table migrations.
var _ = indices.Add(index.PrimaryIndex[RandomEntity]{
	Table:        SingleTable,
	PartitionKey: val.Fmt("world"),
	SortKey:      val.Bytes("SGVsbG8=").Ptr(),
	Secondary: []index.SecondaryIndex{
		{
			GSI:       SingleTable.GSIs[0],
			Partition: val.Fmt("LOL"),
			Sort:      val.Fmt("SAME#{id}").Ptr(),
		},
	},
})
