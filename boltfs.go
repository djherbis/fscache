package fscache

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"gopkg.in/djherbis/stream.v1"
)

type BoltedFs struct {
	db  *bolt.DB
	mfs FileSystem
}

// NewBoltedFs returns a filesystem which uses managedFs to store file data,
// but boltdb to store metadata.
func NewBoltedFs(dbpath string, managedFs FileSystem) (*BoltedFs, error) {
	db, err := bolt.Open(dbpath, 0644, nil)
	if err != nil {
		return nil, err
	}
	return &BoltedFs{db: db, mfs: managedFs}, nil
}

func (fs *BoltedFs) Close() error {
	return fs.db.Close()
}

func (fs *BoltedFs) Reload(add func(key, name string)) error {
	// loop through bolt
	return nil
}

func (fs *BoltedFs) Create(name string) (sf stream.File, err error) {
	fname := md5Str(name)
	return sf, fs.db.Update(func(tx *bolt.Tx) error {
		if sf, err = fs.mfs.Create(fname); err != nil {
			return err
		}
		saveWriteTime(fname, tx)
		saveAccessTime(fname, tx)
		return saveFile(name, fname, tx)
	})
	// TODO(djherbis): rollback file creation on error?
}

func (fs *BoltedFs) Open(name string) (sf stream.File, err error) {
	return sf, fs.db.Update(func(tx *bolt.Tx) error {
		fname, err := getFile(name, tx)
		if err != nil {
			return err
		}
		saveAccessTime(string(fname), tx)
		sf, err = fs.mfs.Open(string(fname))
		return err
	})
}

func (fs *BoltedFs) Remove(name string) error {
	fname := md5Str(name)
	return fs.db.Update(func(tx *bolt.Tx) error {
		deleteFile(fname, tx)
		deleteAccessTime(fname, tx)
		deleteWriteTime(fname, tx)
		return fs.mfs.Remove(fname)
	})
}

func (fs *BoltedFs) RemoveAll() error {
	return fs.db.Update(func(tx *bolt.Tx) error {
		if err := deleteBucket(filenamesBucket, tx); err != nil {
			return err
		}
		if err := deleteBucket(accessTimesBucket, tx); err != nil {
			return err
		}
		if err := deleteBucket(writeTimesBucket, tx); err != nil {
			return err
		}
		return fs.RemoveAll()
	})
}

func (fs *BoltedFs) AccessTimes(name string) (rt, wt time.Time, err error) {
	return rt, wt, fs.db.View(func(tx *bolt.Tx) error {
		rt, err = getAccessTime(name, tx)
		if err == nil {
			wt, err = getWriteTime(name, tx)
		}
		return err
	})
}

func md5Str(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

var filenamesBucket = []byte("filenames")
var accessTimesBucket = []byte("accessTimes")
var writeTimesBucket = []byte("writeTimes")

func getAccessTime(fname string, tx *bolt.Tx) (t time.Time, err error) {
	return getTime(accessTimesBucket, fname, tx)
}

func saveAccessTime(fname string, tx *bolt.Tx) {
	saveTime(accessTimesBucket, fname, tx)
}

func getWriteTime(fname string, tx *bolt.Tx) (t time.Time, err error) {
	return getTime(writeTimesBucket, fname, tx)
}

func saveWriteTime(fname string, tx *bolt.Tx) {
	saveTime(writeTimesBucket, fname, tx)
}

func getTime(timesBucket []byte, fname string, tx *bolt.Tx) (t time.Time, err error) {
	ab := tx.Bucket(timesBucket)
	if ab != nil {
		tdata := ab.Get([]byte(fname))
		var t time.Time
		t.UnmarshalBinary(tdata)
		return t, err
	}
	return t, errors.New("failed")
}

func saveTime(timesBucket []byte, fname string, tx *bolt.Tx) {
	ab := tx.Bucket(timesBucket)
	if ab != nil {
		t, err := time.Now().MarshalBinary()
		if err == nil {
			ab.Put([]byte(fname), t)
		}
	}
}

func deleteWriteTime(fname string, tx *bolt.Tx) error {
	return deleteRecord([]byte(fname), writeTimesBucket, tx)
}

func deleteAccessTime(fname string, tx *bolt.Tx) error {
	return deleteRecord([]byte(fname), accessTimesBucket, tx)
}

func deleteFile(fname string, tx *bolt.Tx) error {
	return deleteRecord([]byte(fname), filenamesBucket, tx)
}

func saveFile(name, fname string, tx *bolt.Tx) error {
	b, err := tx.CreateBucketIfNotExists(filenamesBucket)
	if err != nil {
		return err
	}
	return b.Put([]byte(name), []byte(fname))
}

func getFile(fname string, tx *bolt.Tx) ([]byte, error) {
	return getRecord(filenamesBucket, []byte(fname), tx)
}

func getRecord(bucketName, recordName []byte, tx *bolt.Tx) ([]byte, error) {
	b := tx.Bucket(bucketName)
	if b == nil {
		return nil, bolt.ErrBucketNotFound
	}
	fname := b.Get([]byte(recordName))
	if fname == nil {
		return nil, os.ErrNotExist
	}
	return fname, nil
}

func deleteRecord(bucketName, recordName []byte, tx *bolt.Tx) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return nil
	}
	return b.Delete(recordName)
}

func deleteBucket(bucketName []byte, tx *bolt.Tx) error {
	err := tx.DeleteBucket(bucketName)
	if err != nil && err != bolt.ErrBucketNotFound {
		return err
	}
	return nil
}
