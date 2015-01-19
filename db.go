package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/boltdb/bolt"
	"reflect"
	"strings"
)

// Our database currently only supports uint64, int64, uint, int and string
// datatypes to simplify the serialization process. We are dealing primarily
// with embedded systems running Lua, which does not support floats or doubles.
// Because collections/buckets are cheap to create, we can afford to represent
// arrays/maps/other data structures as those key/value pairs.
type Record struct {
	U64   uint64
	I64   int64
	U     uint
	I     int
	S     string
	Which int // 0 = U64, 1 = I64, etc. Max is 4 = S
}

// Represents an instance to the Bolt instance that represents
// the actual database file on-disk
type DB struct {
	filename    string
	db          *bolt.DB
	nodebuckets map[string]struct{} // keep track of which nodes have buckets
}

// The DB struct provides some convenience functions for the mpdb instance
func NewDB(filename string) *DB {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		log.Critical("Error creating/opening database (%v)", err)
		return nil
	}
	gob.Register(Record{})
	return &DB{filename: filename, db: db,
		nodebuckets: make(map[string]struct{})}
}

// Closes connection to the database
func (db *DB) Close() {
	db.db.Close()
}

// The Persist function stores key/value pairs for a single node. These values
// cannot be shared and can only be read from or written to from the nodeid
// that created them. Keys that already exist in the persist bucket for this
// node will be overwritten
func (db *DB) Persist(nodeid string, data map[string]interface{}) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, nodeid)
		if err != nil {
			return err
		}
		// insert data
		for k, v := range data {
			v_bytes, err := db.encodeInterface(v)
			if err != nil {
				return fmt.Errorf("Could not encode value %s as bytes (%s)", v, err)
			}
			err = b.Put([]byte(k), v_bytes)
			if err != nil {
				return fmt.Errorf("Could not insert key %s value %s for nodeid %s (%s)", k, v, nodeid, err)
			}
		}
		return nil
	})
	return err
}

// GetPersist returns a map[string]interface{} for all keys of the input list
// [keys] that have values in the Persist bucket for the given nodeid. If a
// given key does not have a value, then its entry in the returned map will be
// nil. If [keys] is empty, returns all values in the bucket
func (db *DB) GetPersist(nodeid string, keys []string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	err := db.db.View(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, nodeid)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			for _, key := range keys {
				val, err := db.decodeInterface(b.Get([]byte(key)))
				if err != nil {
					return fmt.Errorf("Could not decode bytes for value (%s)", err)
				}
				result[key] = val
			}
		} else {
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				val, err := db.decodeInterface(v)
				if err != nil {
					return fmt.Errorf("Could not decode bytes for value (%s)", err)
				}
				result[string(k)] = val
			}
		}
		return nil
	})
	return result, err
}

// Insert takes a map of key/value pairs to commit to the database. MPDB
// supports a notion of "collections": a key can have a single prefix (e.g.
// "prefix.key"), which will place the key in the bucket [prefix]. If a key
// does not have a prefix, then it will be stored in the bucket "global".
// There is no collision detection, so any keys that already exist in the
// bucket will be overwritten
func (db *DB) Insert(data map[string]interface{}) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		var (
			parts      []string
			bucketname string
			key        string
		)
		// insert data
		for k, v := range data {
			if strings.Contains(k, ".") { // has prefix
				parts = strings.SplitN(k, ".", 2)
				bucketname = parts[0]
				key = parts[1]
			} else {
				bucketname = "global"
				key = k
			}
			b, err := db.getBucket(tx, bucketname)
			if err != nil {
				return err
			}
			v_bytes, err := db.encodeInterface(v)
			if err != nil {
				return fmt.Errorf("Could not encode value %s as bytes (%s)", v, err)
			}
			err = b.Put([]byte(key), v_bytes)
			if err != nil {
				return fmt.Errorf("Could not insert key %s value %s for nodeid %s (%s)", k, v, bucketname, err)
			}

		}
		return nil
	})
	return err
}

// Returns a k/v map for each of the provided list of keys [keys]. Each key can be
// prefixed to indicate fetching the key from a particular collection. Non-prefixed
// keys will be drawn from the bucket "global". Keys that do not have corresponding values
// will be included in the return map, but will have nil as their value. For fetching
// all key/value pairs for a given bucket, use the GetBucket method. All keys not in the global
// collection will be prefixed with their collection name
func (db *DB) Get(keys []string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	err := db.db.View(func(tx *bolt.Tx) error {
		var (
			parts      []string
			bucketname string
			key        string
		)
		for _, k := range keys {
			if strings.Contains(k, ".") { // has prefix
				parts = strings.SplitN(k, ".", 2)
				bucketname = parts[0]
				key = parts[1]
			} else {
				bucketname = "global"
				key = k
			}
			b, err := db.getBucket(tx, bucketname)
			if err != nil {
				return err
			}
			val, err := db.decodeInterface(b.Get([]byte(key)))
			if err != nil {
				return fmt.Errorf("Could not decode bytes for value (%s)", err)
			}
			if bucketname == "global" {
				result[key] = val
			} else {
				result[bucketname+"."+key] = val
			}
		}
		return nil
	})
	return result, err
}

// Returns a k/v map of all values in the collection with the provided name.
// Each key will be prefixed with the name of the collection, so in a collection
// called "names" with keys "a", "b" and "c", the returned map will have keys
// "names.a", "names.b", "names.c"
func (db *DB) GetBucket(bucketname string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	err := db.db.View(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, bucketname)
		if err != nil {
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			val, err := db.decodeInterface(v)
			if err != nil {
				return fmt.Errorf("Could not decode bytes for value (%s)", err)
			}
			result[bucketname+"."+string(k)] = val
		}
		return nil
	})
	return result, err
}

// encodes arbitrary interface as bytes for safe storage in bolt
func (db *DB) encodeInterface(value interface{}) ([]byte, error) {
	var buf = new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	rec := Record{}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Uint64:
		rec.U64 = value.(uint64)
		rec.Which = 0
	case reflect.Int64:
		rec.I64 = value.(int64)
		rec.Which = 1
	case reflect.Int:
		rec.I = value.(int)
		rec.Which = 2
	case reflect.Uint:
		rec.U = value.(uint)
		rec.Which = 3
	case reflect.String:
		rec.S = value.(string)
		rec.Which = 4
	}
	err := enc.Encode(rec)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decodes the value and returns the primitive
func (db *DB) decodeInterface(value []byte) (interface{}, error) {
	var rec Record
	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&rec)
	if err != nil {
		return nil, err
	}
	switch rec.Which {
	case 0:
		return rec.U64, nil
	case 1:
		return rec.I64, nil
	case 2:
		return rec.I, nil
	case 3:
		return rec.U, nil
	case 4:
		return rec.S, nil
	default:
		return nil, fmt.Errorf("no valid value")
	}
	return rec, nil
}

// fetches or creates bucket with name [name] for the duration of transaction [tx]
func (db *DB) getBucket(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	var b *bolt.Bucket
	var err error
	if _, found := db.nodebuckets[name]; found || !tx.Writable() {
		b = tx.Bucket([]byte(name))
	} else if tx.Writable() {
		b, err = tx.CreateBucketIfNotExists([]byte(name))
	}
	if b == nil {
		return nil, fmt.Errorf("Bucket does not exist")
	}
	// handle error
	if err != nil {
		return b, fmt.Errorf("Could not fetch or create bucket %s (%s)", name, err)
	}
	return b, nil
}
