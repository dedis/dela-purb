package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"sync"

	"golang.org/x/xerrors"
)

// bucket   // key   // value
type db map[string]*dpBucket

// DB is the DELA/PURB implementation of the KV database.
//
// - implements kv.DB
type purbDB struct {
	sync.Mutex
	dbFilePath string
	db         db
	purbIsOn   bool
	blob       *Blob
}

// NewDB opens a new database to the given file.
func NewDB(path string, purbIsOn bool) (DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, xerrors.Errorf("failed to open DB file: %v", err)
	}
	defer f.Close()

	stats, _ := f.Stat()
	s := stats.Size()
	var data = make([]byte, s)
	l, err := f.Read(data)
	if int64(l) < s || err != nil {
		return nil, xerrors.Errorf("failed to read DB file: %v", err)
	}

	p := &purbDB{
		dbFilePath: path,
		db:         make(db),
		purbIsOn:   purbIsOn,
		blob:       NewBlob(),
	}

	p.Lock() // unlocked in Close()

	buffer := bytes.NewBuffer(data)
	if purbIsOn {
		decrypted, err := p.blob.Decode(data)
		if err != nil {
			return nil, err
		}
		buffer.Write(decrypted)
	}

	err = p.deserialize(buffer)
	if fmt.Sprint(err) != "EOF" {
		return nil, xerrors.Errorf("failed to initialize new DB file: %v", err)
	}

	return p, nil
}

// View implements kv.DB. It executes the read-only transaction in the context
// of the database.
func (p *purbDB) View(fn func(ReadableTx) error) error {
	tx := &dpTx{db: p.db}

	err := fn(tx)

	if err != nil {
		return err
	}

	return nil
}

// Update implements kv.DB. It executes the writable transaction in the context
// of the database.
func (p *purbDB) Update(fn func(WritableTx) error) error {
	tx := &dpTx{db: p.db}

	err := fn(tx)
	if err != nil {
		return err
	}

	err = p.savePurbified()
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
	p.Unlock() // locked in NewDB()
	return nil
}

// ---------------------------------------------------------------------------
// helper functions

func (p *purbDB) serialize() (bytes.Buffer, error) {
	var data bytes.Buffer
	encoder := gob.NewEncoder(&data)

	err := encoder.Encode(p.db)
	return data, err
}

func (p *purbDB) deserialize(input *bytes.Buffer) error {
	decoder := gob.NewDecoder(input)

	err := decoder.Decode(&p.db)

	for _, x := range p.db {
		x.updateIndex()
	}

	return err
}

func (p *purbDB) savePurbified() error {
	data, err := p.serialize()
	if err != nil {
		return xerrors.Errorf("failed to serialize DB file: %v", err)
	}

	if p.purbIsOn {
		blob, err := p.blob.Encode(data.Bytes())
		if err != nil {
			return xerrors.Errorf("failed to purbify DB file: %v", err)
		}
		data.Write(blob)
	}

	err = os.WriteFile(p.dbFilePath, data.Bytes(), 0755)
	if err != nil {
		return xerrors.Errorf("failed to save DB file: %v", err)
	}

	return nil
}
