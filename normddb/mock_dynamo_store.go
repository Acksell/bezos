package normddb

func newMockStore(defs ...TableDefinition) *mockStore {
	tables := make(map[string]table)
	for _, t := range defs {
		tables[t.Name] = table{
			name:  t,
			store: make(map[partitionKey][]DynamoEntity),
		}
	}
	return &mockStore{
		tables: make(map[string]*table),
	}
}

type mockStore struct {
	tables map[string]*table
}

type mockTable struct {
	name  TableDefinition
	store map[partitionKey][]DynamoEntity
}

type partitionKey string
