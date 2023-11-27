package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go.dedis.ch/libpurb/libpurb"
	"golang.org/x/xerrors"
)

type privateRWMutex struct {
	sync.RWMutex
}

type bucketDb struct {
	privateRWMutex
	Db map[string]*dpBucket
}

func newBucketDb() bucketDb {
	return bucketDb{Db: make(map[string]*dpBucket)}
}

// DB is the DELA/PURB implementation of the KV database.
//
// - implements kv.DB
type purbDB struct {
	dbFile   string
	bucketDb bucketDb
	blob     *libpurb.Purb
	purbIsOn bool
}

// NewDB opens a new database to the given file.
func NewDB(path string, purbIsOn bool) (DB, error) {
	var filePath string
	if purbIsOn {
		filePath = filepath.Join(path, "purb.db")
	} else {
		filePath = filepath.Join(path, "kv.db")
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, xerrors.Errorf("failed to open DB file: %v", err)
	}
	defer f.Close()

	var b *libpurb.Purb = nil
	if purbIsOn {
		b = NewBlob(path)
	}

	p := &purbDB{
		dbFile:   filePath,
		bucketDb: newBucketDb(),
		purbIsOn: purbIsOn,
		blob:     b,
	}

	stats, _ := f.Stat()
	s := stats.Size()
	if s > 0 {
		err = p.load()
		if err != nil {
			return nil, xerrors.Errorf("failed to load DB file: %v", err)
		}
	}
	return p, nil
}

// View implements kv.DB. It executes the read-only transaction in the context
// of the database.
func (p *purbDB) View(fn func(ReadableTx) error) error {
	tx := &dpTx{db: &p.bucketDb, new: newBucketDb()}

	tx.db.RLock()
	err := fn(tx)
	tx.db.RUnlock()

	if err != nil {
		return err
	}

	return nil
}

// Update implements kv.DB. It executes the writable transaction in the context
// of the database.
func (p *purbDB) Update(fn func(WritableTx) error) error {
	tx := &dpTx{db: &p.bucketDb, new: newBucketDb()}

	tx.db.RLock()
	err := fn(tx)
	tx.db.RUnlock()

	if err != nil {
		return err
	}

	p.bucketDb.Lock()
	for k, v := range tx.new.Db {
		p.bucketDb.Db[k] = v
	}
	p.bucketDb.Unlock()

	err = p.save()
	if err != nil {
		return err
	}

	if tx.onCommit != nil {
		tx.onCommit()
	}

	return nil
}

// Close implements kv.DB. It closes the database. Any view or update call will
// result in an error after this function is called.
func (p *purbDB) Close() error {
	return nil
}

// ---------------------------------------------------------------------------
// helper functions

func (p *purbDB) serialize() (*bytes.Buffer, error) {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)

	p.bucketDb.RLock()
	defer p.bucketDb.RUnlock()
	err := encoder.Encode(p.bucketDb.Db)
	return &data, err
}

func (p *purbDB) deserialize(input *bytes.Buffer) error {
	decoder := gob.NewDecoder(input)

	err := decoder.Decode(&p.bucketDb.Db)

	for _, x := range p.bucketDb.Db {
		x.updateIndex()
	}

	return err
}

func (p *purbDB) save() error {
	data, err := p.serialize()
	if err != nil {
		return xerrors.Errorf("failed to serialize DB file: %v", err)
	}

	if p.purbIsOn {
		blob, err := Encode(p.blob, data.Bytes())
		if err != nil {
			return xerrors.Errorf("failed to purbify DB file: %v", err)
		}
		data = bytes.NewBuffer(blob)
	}

	err = os.WriteFile(p.dbFile, data.Bytes(), 0755)
	if err != nil {
		return xerrors.Errorf("failed to save DB file: %v", err)
	}

	return nil
}

func (p *purbDB) load() error {
	data, err := os.ReadFile(p.dbFile)
	if err != nil {
		return xerrors.Errorf("failed to load DB from file: %v", err)
	}

	if p.purbIsOn && len(data) > 0 {
		data, err = Decode(p.blob, data)
		if err != nil {
			return xerrors.Errorf("failed to decode purbified DB file: %v", err)
		}
	}

	buffer := bytes.NewBuffer(data)
	err = p.deserialize(buffer)
	if err != nil && errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
