package fscache

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	prefixSize   = 8
	expiryPeriod = time.Hour
	reaperPeriod = time.Hour
)

type Cache struct {
	mu     sync.RWMutex
	dir    string
	expiry time.Duration
	files  map[string]*cachedFile
}

// New creates a new Cache based on directory dir.
// Dir is created if it does not exist, and the files
// in it are loaded into the cache using their filename as their key.
// expiry is the # of hours after which an un-accessed key will be
// removed from the cache.
// an expiry of 0 means never expire.
func New(dir string, mode os.FileMode, expiry int) (*Cache, error) {
	err := os.MkdirAll(dir, mode)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		dir:    dir,
		expiry: time.Duration(expiry) * expiryPeriod,
		files:  make(map[string]*cachedFile),
	}
	err = c.load()
	if err != nil {
		return nil, err
	}
	if expiry > 0 {
		c.reaper()
	}
	return c, nil
}

func (c *Cache) reaper() {
	c.reap()
	time.AfterFunc(reaperPeriod, c.reaper)
}

func (c *Cache) reap() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, f := range c.files {
		if atomic.LoadInt64(&f.cnt) > 0 {
			continue
		}

		fi, err := os.Stat(f.name)
		if err != nil {
			continue
		}

		if !time.Now().Add(-c.expiry).Before(atime(fi)) {
			delete(c.files, key)
			return os.Remove(f.name)
		}
	}
	return nil
}

func (c *Cache) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	files, err := ioutil.ReadDir(c.dir)
	if err != nil {
		return err
	}

	modtimes := make(map[string]time.Time)

	for _, f := range files {
		key := getKey(f.Name())
		t, ok := modtimes[key]
		if !ok || t.Before(f.ModTime()) {
			if ok {
				f, ok := c.files[key]
				if ok {
					os.Remove(f.name)
				}
			}
			c.files[key] = oldFile(filepath.Join(c.dir, f.Name()))
			modtimes[key] = f.ModTime()
		} else {
			os.Remove(f.Name())
		}
	}
	return nil
}

func makeName(key string) string {
	buf := bytes.NewBuffer(nil)
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	io.CopyN(enc, rand.Reader, prefixSize)
	return fmt.Sprintf("%s%s", buf.Bytes(), key)
}

func getKey(name string) (key string) {
	if len(name) >= prefixSize {
		key = name[prefixSize:]
	}
	return key
}

// Exists checks if a key is in the cache.
// It is safe to call Exists concurrently with Get.
func (c *Cache) Exists(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.files[key]
	return ok
}

// Get manages access to the streams in the cache.
// If the key does not exist, w != nil and you can start writing to the stream.
// If the key does exist, w == nil.
// r will always be non-nil as long as err == nil and you must close r when you're done reading.
// Get can be called concurrently, and writing and reading is concurrent safe.
func (c *Cache) Get(key string) (r io.ReadCloser, w io.WriteCloser, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	f, ok := c.files[key]
	if !ok {
		f, err = newFile(filepath.Join(c.dir, makeName(key)))
		if err != nil {
			return nil, nil, err
		}
		w = f
		c.files[key] = f
	}
	r, err = f.next()

	return r, w, err
}

// Remove deletes the stream from the cache, blocking until the underlying
// file can be deleted (all active streams finish with it).
// It is safe to call Remove concurrently with Get.
func (c *Cache) Remove(key string) error {
	c.mu.Lock()
	f, ok := c.files[key]
	delete(c.files, key)
	c.mu.Unlock()

	if ok {
		f.grp.Wait()
		return os.Remove(f.name)
	}
	return nil
}

// Clean will empty the cache and delete the cache folder.
// Clean is not safe to call while streams are being read/written.
func (c *Cache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = make(map[string]*cachedFile)
	return os.RemoveAll(c.dir)
}

type cachedFile struct {
	name string
	cnt  int64
	grp  sync.WaitGroup
	w    *os.File
	b    *broadcaster
}

func newFile(name string) (*cachedFile, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{
		name: f.Name(),
		w:    f,
		b:    newBroadcaster(),
	}
	cf.grp.Add(1)
	atomic.AddInt64(&cf.cnt, 1)
	return cf, nil
}

func oldFile(name string) *cachedFile {
	b := newBroadcaster()
	b.Close()
	return &cachedFile{
		name: name,
		b:    b,
	}
}

func (f *cachedFile) next() (r io.ReadCloser, err error) {
	r, err = os.Open(f.name)
	if err == nil {
		f.grp.Add(1)
		atomic.AddInt64(&f.cnt, 1)
	}
	return &cacheReader{
		grp: &f.grp,
		cnt: &f.cnt,
		r:   r,
		b:   f.b,
	}, err
}

func (f *cachedFile) Write(p []byte) (int, error) {
	defer f.b.Broadcast()
	f.b.Lock()
	defer f.b.Unlock()
	return f.w.Write(p)
}

func (f *cachedFile) Close() error {
	defer f.grp.Done()
	defer func() { atomic.AddInt64(&f.cnt, -1) }()
	defer f.b.Close()
	return f.w.Close()
}

type cacheReader struct {
	r   io.ReadCloser
	grp *sync.WaitGroup
	cnt *int64
	b   *broadcaster
}

func (r *cacheReader) Read(p []byte) (n int, err error) {
	r.b.RLock()
	defer r.b.RUnlock()

	for {

		n, err = r.r.Read(p)

		if r.b.IsOpen() { // file is still being written to

			if n != 0 && err == nil { // successful read
				return n, nil
			} else if err == io.EOF { // no data read, wait for some
				r.b.RUnlock()
				r.b.Wait()
				r.b.RLock()
			} else if err != nil { // non-nil, non-eof error
				return n, err
			}

		} else { // file is closed, just return
			return n, err
		}

	}

	return n, err
}

func (r *cacheReader) Close() error {
	defer r.grp.Done()
	defer func() { atomic.AddInt64(r.cnt, -1) }()
	return r.r.Close()
}
