package index

import (
	"testing"

	"github.com/acksell/bezos/dynamodb/index/val"
	"github.com/acksell/bezos/dynamodb/table"
)

type TestEntity struct {
	ID    string `dynamodbav:"id"`
	Email string `dynamodbav:"email"`
}

func TestPrimaryIndex_Validate(t *testing.T) {
	tests := []struct {
		name    string
		idx     PrimaryIndex[TestEntity]
		wantErr bool
	}{
		{
			name: "valid index with sort key",
			idx: PrimaryIndex[TestEntity]{
				Table: table.TableDefinition{
					Name: "TestTable",
					KeyDefinitions: table.PrimaryKeyDefinition{
						PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
						SortKey:      table.KeyDef{Name: "sk", Kind: table.KeyKindS},
					},
				},
				PartitionKey: val.Fmt("USER#{id}"),
				SortKey:      val.Fmt("PROFILE").Ptr(),
			},
			wantErr: false,
		},
		{
			name: "valid index without sort key",
			idx: PrimaryIndex[TestEntity]{
				Table: table.TableDefinition{
					Name: "TestTable",
					KeyDefinitions: table.PrimaryKeyDefinition{
						PartitionKey: table.KeyDef{Name: "pk", Kind: table.KeyKindS},
					},
				},
				PartitionKey: val.Fmt("USER#{id}"),
			},
			wantErr: false,
		},
		{
			name: "missing table name",
			idx: PrimaryIndex[TestEntity]{
				PartitionKey: val.Fmt("USER#{id}"),
			},
			wantErr: true,
		},
		{
			name: "missing partition key",
			idx: PrimaryIndex[TestEntity]{
				Table: table.TableDefinition{Name: "TestTable"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.idx.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrimaryIndex_TableName(t *testing.T) {
	idx := PrimaryIndex[TestEntity]{
		Table: table.TableDefinition{Name: "MyTable"},
	}
	if got := idx.TableName(); got != "MyTable" {
		t.Errorf("TableName() = %q, want %q", got, "MyTable")
	}
}

func TestSecondaryIndex_Validate(t *testing.T) {
	tests := []struct {
		name    string
		gsi     SecondaryIndex
		wantErr bool
	}{
		{
			name: "valid GSI",
			gsi: SecondaryIndex{
				Name: "ByEmail",
				Partition: KeyValDef{
					KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
					ValDef: val.Fmt("EMAIL#{email}"),
				},
			},
			wantErr: false,
		},
		{
			name: "valid GSI with sort key",
			gsi: SecondaryIndex{
				Name: "ByEmail",
				Partition: KeyValDef{
					KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
					ValDef: val.Fmt("EMAIL#{email}"),
				},
				Sort: &KeyValDef{
					KeyDef: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindS},
					ValDef: val.Fmt("USER#{id}"),
				},
			},
			wantErr: false,
		},
		{
			name:    "missing name",
			gsi:     SecondaryIndex{},
			wantErr: true,
		},
		{
			name: "missing partition key def name",
			gsi: SecondaryIndex{
				Name: "ByEmail",
				Partition: KeyValDef{
					ValDef: val.Fmt("EMAIL#{email}"),
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.gsi.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSecondaryIndex_KeyDefinition(t *testing.T) {
	gsi := SecondaryIndex{
		Name: "ByEmail",
		Partition: KeyValDef{
			KeyDef: table.KeyDef{Name: "gsi1pk", Kind: table.KeyKindS},
			ValDef: val.Fmt("EMAIL#{email}"),
		},
		Sort: &KeyValDef{
			KeyDef: table.KeyDef{Name: "gsi1sk", Kind: table.KeyKindN},
			ValDef: val.FromField("timestamp"),
		},
	}

	keyDef := gsi.KeyDefinition()
	if keyDef.PartitionKey.Name != "gsi1pk" {
		t.Errorf("PartitionKey.Name = %q, want %q", keyDef.PartitionKey.Name, "gsi1pk")
	}
	if keyDef.SortKey.Name != "gsi1sk" {
		t.Errorf("SortKey.Name = %q, want %q", keyDef.SortKey.Name, "gsi1sk")
	}
	if keyDef.SortKey.Kind != table.KeyKindN {
		t.Errorf("SortKey.Kind = %q, want %q", keyDef.SortKey.Kind, table.KeyKindN)
	}
}
