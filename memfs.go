package fscache

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"time"
)

type memFS struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

func NewMemFs() FileSystem {
	return &memFS{
		files: make(map[string]*memFile),
	}
}

func (fs *memFS) Reload(add func(key, name string)) error {
	return nil
}

func (fs *memFS) AccessTimes(name string) (rt, wt time.Time, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	f, ok := fs.files[name]
	if ok {
		return f.rt, f.wt, nil
	}
	return rt, wt, errors.New("file has not been read")
}

func (fs *memFS) Create(name string) (File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[name]; ok {
		return nil, errors.New("file exists")
	}
	f := &memFile{
		name: name,
		r:    bytes.NewBuffer(nil),
		wt:   time.Now(),
	}
	fs.files[name] = f
	return f, nil
}

func (fs *memFS) Open(name string) (io.ReadCloser, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[name]; ok {
		f.rt = time.Now()
		return &memReader{memFile: f}, nil
	}
	return nil, errors.New("file does not exist")
}

func (fs *memFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.files, name)
	return nil
}

func (fs *memFS) RemoveAll() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files = nil
	return nil
}

type memFile struct {
	mu     sync.RWMutex
	name   string
	r      *bytes.Buffer
	rt, wt time.Time
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) Write(p []byte) (int, error) {
	if len(p) > 0 {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.r.Write(p)
	}
	return len(p), nil
}

func (f *memFile) Bytes() []byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.r.Bytes()
}

func (f *memFile) Close() error {
	return nil
}

type memReader struct {
	*memFile
	n int
}

func (r *memReader) Read(p []byte) (n int, err error) {
	n, err = bytes.NewReader(r.Bytes()[r.n:]).Read(p)
	r.n += n
	return n, err
}

func (r *memReader) Close() error {
	return nil
}
