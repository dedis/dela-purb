package kv

import (
	"golang.org/x/exp/maps"
	"golang.org/x/xerrors"
)

// A transaction is a list of {key, value}

// dpTx implements kv.ReadableTx and kv.WritableTx
type dpTx struct {
	db       *bucketDb
	new      bucketDb
	onCommit func()
}

// GetBucket implements kv.ReadableTx. It returns the bucket with the given name
// or nil if it does not exist.
func (tx *dpTx) GetBucket(name []byte) Bucket {
	_, found := tx.new.Db[string(name)]
	if found {
		return tx.new.Db[string(name)]
	}

	tx.db.RLock()
	defer tx.db.RUnlock()
	oldBucket, found := tx.db.Db[string(name)]
	if found {
		tx.new.Db[string(name)] = &dpBucket{
			make(kv),
			kOrder{},
		}
		maps.Copy(tx.new.Db[string(name)].Kv, oldBucket.Kv)
		tx.new.Db[string(name)].updateIndex()

		return tx.new.Db[string(name)]
	}

	return nil
}

// GetBucketOrCreate implements kv.WritableTx. It creates the bucket if it does
// not exist and then return it.
func (tx *dpTx) GetBucketOrCreate(name []byte) (Bucket, error) {
	if name == nil {
		return nil, xerrors.New("create bucket failed: bucket name required")
	}

	if len(name) == 0 {
		return nil, xerrors.New("create bucket failed: bucket name required")
	}

	bucket := tx.GetBucket(name)

	if bucket != nil {
		return bucket, nil
	}

	tx.new.Db[string(name)] = &dpBucket{
		make(kv),
		kOrder{},
	}

	return tx.new.Db[string(name)], nil
}

// OnCommit implements store.Transaction. It registers a callback that is called
// after the transaction is successful.
func (tx *dpTx) OnCommit(fn func()) {
	tx.onCommit = fn
}
