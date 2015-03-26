package fscache

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type Cache struct {
	mu    sync.Mutex
	dir   string
	files map[string]*cachedFile
}

func New(dir string) (*Cache, error) {
	err := os.MkdirAll(dir, 0666)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		dir:   dir,
		files: make(map[string]*cachedFile),
	}
	return c, c.load()
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

func (c *Cache) Get(key string) (r io.ReadCloser, w io.WriteCloser, ok bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	f, ok := c.files[key]
	if !ok {
		f, err = newFile(filepath.Join(c.dir, key))
		w = f
		c.files[key] = f
	} else {
		r, err = f.next()
	}

	return r, w, ok, err
}

func (c *Cache) Remove(key string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if f, ok := c.files[key]; ok {
		err = os.Remove(f.name)
		delete(c.files, key)
	}

	return err
}

func (c *Cache) Clean() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = make(map[string]*cachedFile)
	return os.RemoveAll(c.dir)
}

type cachedFile struct {
	name string
	w    *os.File
	b    *broadcaster
}

func newFile(key string) (*cachedFile, error) {
	f, err := os.Create(key)
	return &cachedFile{
		name: key,
		w:    f,
		b:    newBroadcaster(),
	}, err
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
	return &cacheReader{
		r: r,
		b: f.b,
	}, err
}

func (f *cachedFile) Write(p []byte) (int, error) {
	defer f.b.Broadcast()
	f.b.Lock()
	defer f.b.Unlock()
	return f.w.Write(p)
}

func (f *cachedFile) Close() error {
	defer f.b.Close()
	return f.w.Close()
}

type cacheReader struct {
	r io.ReadCloser
	b *broadcaster
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
	return r.r.Close()
}
