package example

import (
	"bezos"
	bzoddb "bezos/dynamodb/ddbsdk"
	"bezos/dynamodb/table"
	"context"
	"fmt"
)

var Table = table.TableDefinition{
	Name: "example",
	KeyDefinitions: table.PrimaryKeyDefinition{
		PartitionKey: table.KeyDef{
			Name: "pk",
			Kind: table.KeyKindS,
		},
		SortKey: table.KeyDef{
			Name: "sk",
			Kind: table.KeyKindS,
		},
	},
	// Projections: []table.Projection{
	// 	table.SecondaryIndexDefinition{},
	// },
}

var MainIndex = table.PrimaryIndexDefinition{
	Table:          Table,
	PartitionKeyer: table.FmtKeyer("%s", "id"),
}

var VersionIndex = table.PrimaryIndexDefinition{
	Table:          Table,
	PartitionKeyer: table.FmtKeyer("ID#%s", "id"),
	SortKeyer:      table.FmtKeyer("VERSION#%s", "meta.version"),
}

type exampleEntity struct {
	ID    string            `json:"id"`
	Meta  *bezos.EntityMeta `json:"meta"`
	Value string            `json:"value"`
}

func (e exampleEntity) GetMeta() *bezos.EntityMeta {
	return e.Meta
}

func (e exampleEntity) GetID() string {
	return e.ID
}

func (e exampleEntity) IsValid() error {
	if e.ID == "" {
		return fmt.Errorf("id is required")
	}
	if e.Value == "" {
		return fmt.Errorf("value is required")
	}
	return nil
}

func (e exampleEntity) Version() (string, any) {
	return "meta.observed", e.Meta.Observed
}

func _() {
	ctx := context.Background()

	var c bzoddb.Client

	// todo optional way to run a transaction
	// err := c.RunTx(func(tx bzoddb.Txer) {
	// 	example := &exampleEntity{"123", nil, "foo"}
	// 	tx.AddAction(ctx, bzoddb.NewSafePut(VersionIndex, example))
	// })
	// if err != nil {
	// 	panic(err)
	// }

	tx := c.NewTx()
	example := &exampleEntity{"123", nil, "foo"}
	tx.AddAction(ctx, bzoddb.NewSafePut(VersionIndex, example))
	err := tx.Commit(ctx)
	if err != nil {
		panic(err)
	}
}

func _() {
	var c bzoddb.Client

	q := c.NewQuery(
		Table,
		bzoddb.NewKeyCondition("state#123", bzoddb.BeginsWith("lol")),
		bzoddb.WithDescending(),
		bzoddb.WithPageSize(10),
		bzoddb.WithEntityFilter("ExampleEntity"))

	ctx := context.Background()
	res, err := q.Next(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(res.Entities))
}
