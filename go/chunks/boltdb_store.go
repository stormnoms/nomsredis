// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"fmt"
	"math"
	"os"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"
	flag "github.com/juju/gnuflag"
	"github.com/stormasm/nomsredis/go/constants"
	"github.com/stormasm/nomsredis/go/d"
	"github.com/stormasm/nomsredis/go/hash"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	//"github.com/syndtr/goleveldb/leveldb/filter"
	//"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/boltdb/bolt"
)

/*
const (
	rootKeyConst     = "/root"
	versionKeyConst  = "/vers"
	chunkPrefixConst = "/chunk/"
)
*/

type BoltDBStoreFlags struct {
	maxFileHandles int
	dumpStats      bool
}

var (
	ldbFlagsBolt        = BoltDBStoreFlags{24, false}
	flagsRegisteredBolt = false
)

func RegisterBoltDBFlags(flags *flag.FlagSet) {
	if !flagsRegisteredBolt {
		flagsRegisteredBolt = true
		flags.IntVar(&ldbFlagsBolt.maxFileHandles, "ldb-max-file-handles", 24, "max number of open file handles")
		flags.BoolVar(&ldbFlagsBolt.dumpStats, "ldb-dump-stats", false, "print get/has/put counts on close")
	}
}

func newBoltDBStore(store *internalBoltDBStore, ns []byte, closeBackingStore bool) *BoltDBStore {
	copyNsAndAppend := func(suffix string) (out []byte) {
		out = make([]byte, len(ns)+len(suffix))
		copy(out[copy(out, ns):], []byte(suffix))
		return
	}
	return &BoltDBStore{
		internalBoltDBStore: store,
		rootKey:             copyNsAndAppend(rootKeyConst),
		versionKey:          copyNsAndAppend(versionKeyConst),
		chunkPrefix:         copyNsAndAppend(chunkPrefixConst),
		closeBackingStore:   closeBackingStore,
	}
}

type BoltDBStore struct {
	*internalBoltDBStore
	rootKey           []byte
	versionKey        []byte
	chunkPrefix       []byte
	closeBackingStore bool
	versionSetOnce    sync.Once
}

func (l *BoltDBStore) Root() hash.Hash {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	return l.rootByKey(l.rootKey)
}

func (l *BoltDBStore) UpdateRoot(current, last hash.Hash) bool {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	l.versionSetOnce.Do(l.setVersIfUnset)
	return l.updateRootByKey(l.rootKey, current, last)
}

func (l *BoltDBStore) Get(ref hash.Hash) Chunk {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	return l.getByKey(l.toChunkKey(ref), ref)
}

func (l *BoltDBStore) Has(ref hash.Hash) bool {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	return l.hasByKey(l.toChunkKey(ref))
}

func (l *BoltDBStore) Version() string {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	return l.versByKey(l.versionKey)
}

func (l *BoltDBStore) Put(c Chunk) {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	l.versionSetOnce.Do(l.setVersIfUnset)
	l.putByKey(l.toChunkKey(c.Hash()), c)
}

func (l *BoltDBStore) PutMany(chunks []Chunk) (e BackpressureError) {
	d.PanicIfFalse(l.internalBoltDBStore != nil, "Cannot use BoltDBStore after Close().")
	l.versionSetOnce.Do(l.setVersIfUnset)
	//numBytes := 0
	//b := new(leveldb.Batch)
	for _, c := range chunks {
		//data := snappy.Encode(nil, c.Data())
		//numBytes += len(data)
		l.Put(c)
	}
	//l.putBatch(b, numBytes)
	return
}

func (l *BoltDBStore) Close() error {
	if l.closeBackingStore {
		l.internalBoltDBStore.Close()
	}
	l.internalBoltDBStore = nil
	return nil
}

func (l *BoltDBStore) toChunkKey(r hash.Hash) []byte {
	digest := r.DigestSlice()
	out := make([]byte, len(l.chunkPrefix), len(l.chunkPrefix)+len(digest))
	copy(out, l.chunkPrefix)
	return append(out, digest...)
}

func (l *BoltDBStore) setVersIfUnset() {
	/*
		exists, err := l.db.Has(l.versionKey, nil)
		d.Chk.NoError(err)
		if !exists {
			l.setVersByKey(l.versionKey)
		}
	*/
	l.setVersByKey(l.versionKey)
}

type internalBoltDBStore struct {
	db                                     *bolt.DB
	mu                                     sync.Mutex
	getCount, hasCount, putCount, putBytes int64
	dumpStats                              bool
}

func newBoltDBBackingStore(dir string, dumpStats bool) *internalBoltDBStore {
	d.PanicIfTrue(dir == "", "dir cannot be empty")
	d.PanicIfError(os.MkdirAll(dir, 0700))
	db, err := bolt.Open("bolt.db", 0644, nil)
	d.Chk.NoError(err, "opening internalBoltDBStore in %s", dir)
	return &internalBoltDBStore{
		db:        db,
		dumpStats: dumpStats,
	}
}

func (l *internalBoltDBStore) rootByKey(key []byte) hash.Hash {
	// val, err := l.db.Get(key, nil)
/*
	val, err := l.viewBolt(key)
	fmt.Println("val = ", val)
	fmt.Println("err = ", err)
	return hash.Hash{}
*/

	val, err := l.viewBolt(key)

	if len(val) == 0 {
		return hash.Hash{}
	}
	d.Chk.NoError(err)

	return hash.Parse(string(val))

}

func (l *internalBoltDBStore) updateRootByKey(key []byte, current, last hash.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last != l.rootByKey(key) {
		return false
	}

	// Sync: true write option should fsync memtable data to disk
	// err := l.db.Put(key, []byte(current.String()), &opt.WriteOptions{Sync: true})

	// need to add in the ability for Bolt sync option
	err := l.updateBolt(key, []byte(current.String()))

	d.Chk.NoError(err)
	return true
}

func (l *internalBoltDBStore) getByKey(key []byte, ref hash.Hash) Chunk {
	//compressed, err := l.db.Get(key, nil)
	compressed, err := l.viewBolt(key)
	l.getCount++
	if err == errors.ErrNotFound {
		return EmptyChunk
	}
	d.Chk.NoError(err)
	data, err := snappy.Decode(nil, compressed)
	d.Chk.NoError(err)
	return NewChunkWithHash(ref, data)
}

func (l *internalBoltDBStore) hasByKey(key []byte) bool {
	// exists, err := l.db.Has(key, &opt.ReadOptions{DontFillCache: true}) // This isn't really a "read", so don't signal the cache to treat it as one.
	exists, err := l.hasBolt(key)
	d.Chk.NoError(err)
	l.hasCount++
	return exists
}

func (l *internalBoltDBStore) versByKey(key []byte) string {
	//val, err := l.db.Get(key, nil)
	val, err := l.viewBolt(key)
	if err == errors.ErrNotFound {
		return constants.NomsVersion
	}
	d.Chk.NoError(err)
	return string(val)
}

func (l *internalBoltDBStore) setVersByKey(key []byte) {
	//err := l.db.Put(key, []byte(constants.NomsVersion), nil)

	err := l.updateBolt(key, []byte(constants.NomsVersion))

	d.Chk.NoError(err)
}

func (l *internalBoltDBStore) updateBolt(key []byte, value []byte) error {
	// store some data
	err := l.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("sam"))
		if err != nil {
			return err
		}

		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (l *internalBoltDBStore) viewBolt(key []byte) (val []byte, err error) {
	// retrieve the data
	err = l.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("sam"))

		if bucket == nil {
			return fmt.Errorf("Bucket sam not found!")
		}

		val = bucket.Get(key)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("viewBolt error")
	}

	fmt.Println("viewBolt return val = ",string(val))
	return val, nil
}

func (l *internalBoltDBStore) hasBolt(key []byte) (bool, error) {
	val, err := l.viewBolt(key)
	if err != nil {
		return false, err
	}
	size := len(val)
	if size > 0 {
		return true, err
	}
	return false, err
}

func (l *internalBoltDBStore) putByKey(key []byte, c Chunk) {
	value := snappy.Encode(nil, c.Data())

	// This was the way with leveldb
	//	err := l.db.Put(key, data, nil)

	err := l.updateBolt(key, value)
	d.Chk.NoError(err)
	l.putCount++
	l.putBytes += int64(len(value))
}

func (l *internalBoltDBStore) putBatch(b *leveldb.Batch, numBytes int) {
	/*
		err := l.db.Write(b, nil)
		d.Chk.NoError(err)
		l.putCount += int64(b.Len())
		l.putBytes += int64(numBytes)
	*/
	fmt.Println("Currently not yet implemented")
}

func (l *internalBoltDBStore) Close() error {
	l.db.Close()
	if l.dumpStats {
		fmt.Println("--Bolt Stats--")
		fmt.Println("GetCount: ", l.getCount)
		fmt.Println("HasCount: ", l.hasCount)
		fmt.Println("PutCount: ", l.putCount)
		fmt.Printf("PutSize:   %s (%d/chunk)\n", humanize.Bytes(uint64(l.putCount)), l.putBytes/int64(math.Max(1, float64(l.putCount))))
	}
	return nil
}

func NewBoltDBStoreFactory(dir string, dumpStats bool) Factory {
	return &BoltDBStoreFactory{dir, dumpStats, newBoltDBBackingStore(dir, dumpStats)}
}

func NewBoltDBStoreFactoryUseFlags(dir string) Factory {
	return NewBoltDBStoreFactory(dir, ldbFlagsBolt.dumpStats)
}

type BoltDBStoreFactory struct {
	dir       string
	dumpStats bool
	store     *internalBoltDBStore
}

func (f *BoltDBStoreFactory) CreateStore(ns string) ChunkStore {
	d.PanicIfFalse(f.store != nil, "Cannot use BoltDBStoreFactory after Shutter().")
	return newBoltDBStore(f.store, []byte(ns), false)
}

func (f *BoltDBStoreFactory) Shutter() {
	f.store.Close()
	f.store = nil
}
