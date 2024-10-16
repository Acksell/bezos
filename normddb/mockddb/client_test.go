package mockddb

import "testing"

var testTable = table.TableDefinition{
	Name: "test-table",
	KeyNames: table.KeyNames{
		PartitionKeyName: "pk",
		SortKeyName:      "sk",
	},
	TimeToLiveKey: "ttl",
	GSIKeys: []table.KeyNames{
		{
			PartitionKeyName: "gsi1pk",
			SortKeyName:      "gsi1sk",
		},
	},
}

func TestPutItem(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store := NewStore()
	})
}
