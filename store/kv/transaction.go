package kv

// A transaction is a list of {key, value}

// dpTx implements kv.ReadableTx and kv.WritableTx
type dpTx struct {
	db       db
	onCommit func()
}

// GetBucket implements kv.ReadableTx. It returns the bucket with the given name
// or nil if it does not exist.
func (tx *dpTx) GetBucket(name []byte) Bucket {
	bucket, found := tx.db[string(name)]
	if found {
		return &dpBucket{bucket}
	}

	return nil
}

// GetBucketOrCreate implements kv.WritableTx. It creates the bucket if it does
// not exist and then return it.
func (tx *dpTx) GetBucketOrCreate(name []byte) (Bucket, error) {
	_, found := tx.db[string(name)]
	if !found {
		tx.db[string(name)] = make(map[string][]byte)
	}

	return &dpBucket{kv: tx.db[string(name)]}, nil
}

// OnCommit implements store.Transaction. It registers a callback that is called
// after the transaction is successful.
func (tx *dpTx) OnCommit(fn func()) {
	tx.onCommit = fn
}
