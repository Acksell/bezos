package normddb

var _ Reader = &dynamock{}

func (db *dynamock) NewGet(opts ...GetOption) Getter {
	//todo implement this
	return nil
}

func (db *dynamock) NewQuery(td TableDefinition, kc KeyCondition, opts ...QueryOption) Querier {
	//todo implement this
	return nil
}
