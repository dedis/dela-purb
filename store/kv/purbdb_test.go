package kv

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

const purbDbTestDir = "purb-db-kv"

func TestPurbDb_OpenClose(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestPurbDb_OpenCloseReopen(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	//reopen
	db, err = NewDB(dir, true)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestPurbDb_UpdateAndView(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	ch := make(chan struct{})
	err = db.Update(func(txn WritableTx) error {
		txn.OnCommit(func() { close(ch) })

		bucket, err := txn.GetBucketOrCreate([]byte("bucket"))
		require.NoError(t, err)

		return bucket.Set([]byte("ping"), []byte("pong"))
	})
	require.NoError(t, err)

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	err = db.View(func(txn ReadableTx) error {
		bucket := txn.GetBucket([]byte("bucket"))
		require.NotNil(t, bucket)

		value := bucket.Get([]byte("ping"))
		require.Equal(t, []byte("pong"), value)

		return nil
	})
	require.NoError(t, err)
}

func TestPurbDb_GetBucket(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(tx WritableTx) error {
		require.Nil(t, tx.GetBucket([]byte("unknown")))

		_, err := tx.GetBucketOrCreate([]byte("A"))
		require.NoError(t, err)
		require.NotNil(t, tx.GetBucket([]byte("A")))

		_, err = tx.GetBucketOrCreate(nil)
		require.EqualError(t, err, "create bucket failed: bucket name required")

		return nil
	})
	require.NoError(t, err)
}

func TestPurbDb_GetSetDelete(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("bucket"))
		require.NoError(t, err)

		require.NoError(t, b.Set([]byte("ping"), []byte("pong")))

		value := b.Get([]byte("ping"))
		require.Equal(t, []byte("pong"), value)

		value = b.Get([]byte("pong"))
		require.Nil(t, value)

		require.NoError(t, b.Delete([]byte("ping")))

		value = b.Get([]byte("ping"))
		require.Nil(t, value)

		return nil
	})

	require.NoError(t, err)
}

func TestPurbDb_SetReopenGet(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("bucket"))
		require.NoError(t, err)

		err = b.Set([]byte("ping"), []byte("pong"))
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	//reopen
	db, err = NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(txn WritableTx) error {
		b := txn.GetBucket([]byte("bucket"))
		require.NotNil(t, b)

		value := b.Get([]byte("ping"))
		require.Equal(t, []byte("pong"), value)

		return nil
	})
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	require.NoError(t, err)
}

func TestPurbDb_ForEach(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, b.Set([]byte{2}, []byte{2}))
		require.NoError(t, b.Set([]byte{1}, []byte{1}))
		require.NoError(t, b.Set([]byte{0}, []byte{0}))

		var i byte = 0
		return b.ForEach(func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i}, v)
			i++
			return nil
		})
	})
	require.NoError(t, err)
}

func TestPurbDb_ForEachAborted(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	// set some values in the DB
	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, b.Set([]byte{2}, []byte{2}))
		require.NoError(t, b.Set([]byte{1}, []byte{1}))
		require.NoError(t, b.Set([]byte{0}, []byte{0}))

		return nil
	})
	require.NoError(t, err)

	// try to alter the DB with an interrupted transaction
	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		err = b.Set([]byte{0}, []byte{7})
		require.NoError(t, err)

		return xerrors.New("testing error")
	})
	require.Error(t, err)

	// checks that the DB values are still ok
	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		var i byte = 0
		return b.ForEach(func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i}, v)
			i++
			return nil
		})
	})
	require.NoError(t, err)
}

func TestPurbDb_ReOpenClosedDb(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	// set some values in the DB
	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		require.NoError(t, b.Set([]byte{2}, []byte{2}))
		require.NoError(t, b.Set([]byte{1}, []byte{1}))
		require.NoError(t, b.Set([]byte{0}, []byte{0}))

		return nil
	})
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// re-open DB file
	NewDB, err := NewDB(dir, true)
	require.NoError(t, err)

	// checks that the DB values are still ok
	err = NewDB.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("test"))
		require.NoError(t, err)

		var i byte = 0
		return b.ForEach(func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i}, v)
			i++
			return nil
		})
	})
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestPurbDb_Scan(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), purbDbTestDir)
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	db, err := NewDB(dir, true)
	require.NoError(t, err)

	err = db.Update(func(txn WritableTx) error {
		b, err := txn.GetBucketOrCreate([]byte("bucket"))
		require.NoError(t, err)

		require.NoError(t, b.Set([]byte{7}, []byte{7}))
		require.NoError(t, b.Set([]byte{0}, []byte{0}))

		var i byte = 0
		err = b.Scan(nil, func(k, v []byte) error {
			require.Equal(t, []byte{i}, k)
			require.Equal(t, []byte{i}, v)
			i += 7
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, byte(14), i)

		err = b.Scan([]byte{1}, func(k, v []byte) error {
			return xerrors.New("callback error")
		})
		require.NoError(t, err)

		err = b.Scan([]byte{}, func(k, v []byte) error {
			return xerrors.New("callback error")
		})
		require.ErrorContains(t, err, "callback error")

		return nil
	})
	require.NoError(t, err)
}
