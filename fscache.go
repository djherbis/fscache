package fscache

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
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
func New(dir string, expiry int) (*Cache, error) {
	err := os.MkdirAll(dir, 0666)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		dir:    dir,
		expiry: time.Duration(expiry) * expiryPeriod,
		files:  make(map[string]*cachedFile),
	}
	time.AfterFunc(reaperPeriod, c.reaper)
	return c, c.load()
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
			delete(c.files, key)
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
	for _, f := range files {
		c.files[f.Name()] = oldFile(filepath.Join(c.dir, f.Name()))
	}
	return nil
}

// Exists checks if a key is in the cache
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
		f, err = newFile(filepath.Join(c.dir, key))
		if err != nil {
			return nil, nil, err
		}
		atomic.AddInt64(&f.cnt, 1)
		w = f
		c.files[key] = f
	}
	r, err = f.next()

	return r, w, err
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
	w    *os.File
	b    *broadcaster
}

func newFile(key string) (*cachedFile, error) {
	f, err := os.Create(key)
	if err != nil {
		return nil, err
	}
	return &cachedFile{
		name: f.Name(),
		w:    f,
		b:    newBroadcaster(),
	}, nil
}

func oldFile(key string) *cachedFile {
	b := newBroadcaster()
	b.Close()
	return &cachedFile{
		name: key,
		b:    b,
	}
}

func (f *cachedFile) next() (r io.ReadCloser, err error) {
	r, err = os.Open(f.name)
	if err == nil {
		atomic.AddInt64(&f.cnt, 1)
	}
	return &cacheReader{
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
	defer func() { atomic.AddInt64(&f.cnt, -1) }()
	defer f.b.Close()
	return f.w.Close()
}

type cacheReader struct {
	r   io.ReadCloser
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
	defer func() { atomic.AddInt64(r.cnt, -1) }()
	return r.r.Close()
}
