package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"

	"go.dedis.ch/kyber/v3/util/key"
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
func NewDB(path string, purbIsOn bool) (DB, []*key.Pair, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to open DB file: %v", err)
	}
	defer f.Close()

	stats, _ := f.Stat()
	s := stats.Size()
	if s > 0 {
		return nil, nil, xerrors.New("failed to create DB file: file already exists")
	}

	var data = make([]byte, s)
	l, err := f.Read(data)
	if int64(l) != 0 || err != nil {
		return nil, nil, xerrors.Errorf("failed to read DB file: %v", err)
	}

	var b *libpurb.Purb = nil
	keypair := make([]*key.Pair, 0)
	if purbIsOn {
		b = NewBlob(nil)
		pair := key.Pair{
			Public:  b.Recipients[0].PublicKey,
			Private: b.Recipients[0].PrivateKey,
		}
		keypair = append(keypair, &pair)
	}

	p := &purbDB{
		dbFile:   path,
		bucketDb: newBucketDb(),
		purbIsOn: purbIsOn,
		blob:     b,
	}

	return p, keypair, nil
}

// LoadDB opens a database from a given file.
func LoadDB(path string, purbIsOn bool, keypair []*key.Pair) (DB, error) {
	var b *libpurb.Purb = nil
	if purbIsOn {
		b = NewBlob(keypair)
	}

	p := &purbDB{
		dbFile:   path,
		bucketDb: newBucketDb(),
		purbIsOn: purbIsOn,
		blob:     b,
	}

	err := p.load()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// View implements kv.DB. It executes the read-only transaction in the context
// of the database.
func (p *purbDB) View(fn func(ReadableTx) error) error {
	tx := &dpTx{db: p.bucketDb, new: newBucketDb()}

	err := fn(tx)

	if err != nil {
		return err
	}

	return nil
}

// Update implements kv.DB. It executes the writable transaction in the context
// of the database.
func (p *purbDB) Update(fn func(WritableTx) error) error {
	tx := &dpTx{db: p.bucketDb, new: newBucketDb()}

	err := fn(tx)
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
