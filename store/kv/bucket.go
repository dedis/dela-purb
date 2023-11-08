package kv

import (
	"slices"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/xerrors"
)

type kv map[string][]byte
type kOrder []string

// dpBucket implements kv.Bucket
type dpBucket struct {
	Kv  kv
	idx kOrder
}

func (b *dpBucket) updateIndex() {
	b.idx = maps.Keys(b.Kv)
	slices.Sort(b.idx)
}

// Get implements kv.Bucket. It returns the value associated to the key, or nil
// if it does not exist.
func (b *dpBucket) Get(key []byte) []byte {
	v, found := b.Kv[string(key)]
	if found {
		return v
	}
	return nil
}

// Set implements kv.Bucket. It sets the provided key to the value.
func (t *dpBucket) Set(key, value []byte) error {
	if _, found := t.Kv[string(key)]; !found {
		t.idx = append(t.idx, string(key))
		slices.Sort(t.idx)
	}

	t.Kv[string(key)] = value

	return nil
}

// Delete implements kv.Bucket. It deletes the key from the bucket.
func (b *dpBucket) Delete(key []byte) error {
	delete(b.Kv, string(key))
	return nil
}

// ForEach implements kv.Bucket. It iterates over the whole bucket in an
// unspecified order. If the callback returns an error, the iteration is stopped
// and the error returned to the caller.
func (b *dpBucket) ForEach(fn func(k, v []byte) error) error {
	for _, k := range b.idx {
		err := fn([]byte(k), b.Kv[k])
		if err != nil {
			return err
		}
	}
	return nil
}

// Scan implements kv.Bucket. It iterates over the keys matching the prefix in a
// sorted order. If the callback returns an error, the iteration is stopped and
// the error returned to the caller.
func (b *dpBucket) Scan(prefix []byte, fn func(k, v []byte) error) error {
	for _, k := range b.idx {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		err := fn([]byte(k), b.Kv[k])
		if err != nil {
			return xerrors.Errorf("failed to scan bucket: %v", err)
		}
	}
	return nil
}
