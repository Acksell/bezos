package bzoddb

var _ Writer = &dynamock{}

func (d *dynamock) NewTx(opts ...TxOption) Txer {
	txo := txOpts{}
	for _, opt := range opts {
		opt(&txo)
	}
	return &mockTxer{
		// store: d.store,
		opts: txo}
}

func (d *dynamock) NewBatch(opts ...BatchOption) Batcher {
	batcho := batchOpts{}
	for _, opt := range opts {
		opt(&batcho)
	}
	return &mockBatcher{
		// store: d.store,
		opts: batcho}
}
