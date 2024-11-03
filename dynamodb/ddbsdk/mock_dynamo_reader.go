package bzoddb

import "bezos/dynamodb/table"

var _ Reader = &dynamock{}

func (db *dynamock) NewGet(opts ...GetOption) Getter {
	//todo implement this
	return nil
}

func (db *dynamock) NewQuery(td table.TableDefinition, kc KeyCondition, opts ...QueryOption) Querier {
	//todo implement this
	return nil
}
