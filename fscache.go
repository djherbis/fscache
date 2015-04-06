package fscache

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Cache works like a concurrent-safe map for streams.
type Cache interface {

	// Get manages access to the streams in the cache.
	// If the key does not exist, w != nil and you can start writing to the stream.
	// If the key does exist, w == nil.
	// r will always be non-nil as long as err == nil and you must close r when you're done reading.
	// Get can be called concurrently, and writing and reading is concurrent safe.
	Get(key string) (io.ReadCloser, io.WriteCloser, error)

	// Remove deletes the stream from the cache, blocking until the underlying
	// file can be deleted (all active streams finish with it).
	// It is safe to call Remove concurrently with Get.
	Remove(key string) error

	// Exists checks if a key is in the cache.
	// It is safe to call Exists concurrently with Get.
	Exists(key string) bool

	// Clean will empty the cache and delete the cache folder.
	// Clean is not safe to call while streams are being read/written.
	Clean() error
}

type cache struct {
	mu    sync.RWMutex
	files map[string]*cachedFile
	grim  Reaper
	fs    FileSystem
}

// New creates a new Cache using NewFs(dir, perms).
// expiry is the duration after which an un-accessed key will be removed from
// the cache, a zero value expiro means never expire.
func New(dir string, perms os.FileMode, expiry time.Duration) (Cache, error) {
	fs, err := NewFs(dir, perms)
	if err != nil {
		return nil, err
	}
	var grim Reaper
	if expiry > 0 {
		grim = &reaper{
			expiry: expiry,
			period: expiry,
		}
	}
	return NewCache(fs, grim)
}

// NewCache creates a new Cache based on FileSystem fs.
// fs.Files() are loaded using the name they were created with as a key.
// Reaper is used to determine when files expire, nil means never expire.
func NewCache(fs FileSystem, grim Reaper) (Cache, error) {
	c := &cache{
		files: make(map[string]*cachedFile),
		grim:  grim,
		fs:    fs,
	}
	err := c.load()
	if err != nil {
		return nil, err
	}
	if grim != nil {
		c.haunter()
	}
	return c, nil
}

func (c *cache) haunter() {
	c.haunt()
	time.AfterFunc(c.grim.Next(), c.haunter)
}

func (c *cache) haunt() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, f := range c.files {
		if atomic.LoadInt64(&f.cnt) > 0 {
			continue
		}

		lastRead, lastWrite, err := c.fs.AccessTimes(f.name)
		if err != nil {
			continue
		}

		if c.grim.Reap(key, lastRead, lastWrite) {
			delete(c.files, key)
			c.fs.Remove(f.name)
		}
	}
	return
}

func (c *cache) load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fs.Reload(func(key, name string) {
		c.files[key] = c.oldFile(name)
	})
}

func (c *cache) Exists(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.files[key]
	return ok
}

func (c *cache) Get(key string) (r io.ReadCloser, w io.WriteCloser, err error) {
	c.mu.RLock()
	f, ok := c.files[key]
	if ok {
		r, err = f.next()
		c.mu.RUnlock()
		return r, nil, err
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	f, ok = c.files[key]
	if ok {
		r, err = f.next()
		return r, nil, err
	}

	f, err = c.newFile(key)
	if err != nil {
		return nil, nil, err
	}

	r, err = f.next()
	if err != nil {
		f.Close()
		c.fs.Remove(f.name)
		return nil, nil, err
	}

	c.files[key] = f

	return r, f, err
}

func (c *cache) Remove(key string) error {
	c.mu.Lock()
	f, ok := c.files[key]
	delete(c.files, key)
	c.mu.Unlock()

	if ok {
		f.grp.Wait()
		return c.fs.Remove(f.name)
	}
	return nil
}

func (c *cache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = make(map[string]*cachedFile)
	return c.fs.RemoveAll()
}

type cachedFile struct {
	fs   FileSystem
	name string
	cnt  int64
	grp  sync.WaitGroup
	w    io.WriteCloser
	b    *broadcaster
}

func (c *cache) newFile(name string) (*cachedFile, error) {
	f, err := c.fs.Create(name)
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{
		fs:   c.fs,
		name: f.Name(),
		w:    f,
		b:    newBroadcaster(),
	}
	cf.grp.Add(1)
	atomic.AddInt64(&cf.cnt, 1)
	return cf, nil
}

func (c *cache) oldFile(name string) *cachedFile {
	b := newBroadcaster()
	b.Close()
	return &cachedFile{
		fs:   c.fs,
		name: name,
		b:    b,
	}
}

func (f *cachedFile) next() (r io.ReadCloser, err error) {
	r, err = f.fs.Open(f.name)
	if err != nil {
		return nil, err
	}
	f.grp.Add(1)
	atomic.AddInt64(&f.cnt, 1)
	return &cacheReader{
		grp: &f.grp,
		cnt: &f.cnt,
		r:   r,
		b:   f.b,
	}, nil
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
}

func (r *cacheReader) Close() error {
	defer r.grp.Done()
	defer func() { atomic.AddInt64(r.cnt, -1) }()
	return r.r.Close()
}
