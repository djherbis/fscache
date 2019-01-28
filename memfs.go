package fscache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/djherbis/stream.v1"
)

type memFS struct {
	mu    sync.RWMutex
	files map[string]*memFile
}

type memFileInfo struct {
	name string
	size int64
	fs   *memFS
}

func (f *memFileInfo) Name() string {
	return f.name
}

func (f *memFileInfo) Size() int64 {
	return f.size
}

func (f *memFileInfo) Mode() os.FileMode {
	return os.ModeIrregular
}

func (f *memFileInfo) ModTime() time.Time {
	_, wt, _ := f.fs.accessTimes(f.Name())
	return wt
}

func (f *memFileInfo) IsDir() bool {
	return false
}

func (f *memFileInfo) Sys() interface{} {
	return nil
}

func (f *memFileInfo) AccessTimes() (rt, wt time.Time, err error) {
	return f.fs.accessTimes(f.Name())
}

// NewMemFs creates an in-memory FileSystem.
// It does not support persistence (Reload is a nop).
func NewMemFs() FileSystem {
	return &memFS{
		files: make(map[string]*memFile),
	}
}

func (fs *memFS) Stat(name string) (FileInfo, error) {
	size, err := fs.size(name)
	if err != nil {
		return nil, err
	}

	return &memFileInfo{name: name, size: size, fs: fs}, nil
}

func (fs *memFS) size(name string) (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	f, ok := fs.files[name]
	if ok {
		return int64(len(f.Bytes())), nil
	}
	return 0, errors.New("file has not been read")
}

func (fs *memFS) Reload(add func(key, name string)) error {
	return nil
}

func (fs *memFS) accessTimes(name string) (rt, wt time.Time, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	f, ok := fs.files[name]
	if ok {
		return f.rt, f.wt, nil
	}
	return rt, wt, errors.New("file has not been read")
}

func (fs *memFS) Create(key string) (stream.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.files[key]; ok {
		return nil, errors.New("file exists")
	}
	file := &memFile{
		name: key,
		r:    bytes.NewBuffer(nil),
		wt:   time.Now(),
	}
	file.memReader.memFile = file
	fs.files[key] = file
	return file, nil
}

func (fs *memFS) Open(name string) (stream.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[name]; ok {
		f.rt = time.Now()
		return &memReader{memFile: f}, nil
	}
	return nil, errors.New("file does not exist")
}

func (fs *memFS) Remove(key string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.files, key)
	return nil
}

func (fs *memFS) RemoveAll() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files = make(map[string]*memFile)
	return nil
}

type memFile struct {
	mu   sync.RWMutex
	name string
	r    *bytes.Buffer
	memReader
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

func (r *memReader) ReadAt(p []byte, off int64) (n int, err error) {
	data := r.Bytes()
	if int64(len(data)) < off {
		return 0, io.EOF
	}
	n, err = bytes.NewReader(data[off:]).ReadAt(p, 0)
	return n, err
}

func (r *memReader) Read(p []byte) (n int, err error) {
	n, err = bytes.NewReader(r.Bytes()[r.n:]).Read(p)
	r.n += n
	return n, err
}

func (r *memReader) Close() error {
	return nil
}
