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
	"time"
)

const prefixSize = 8

// FileSystem is used as the source for a Cache.
type FileSystem interface {
	Files() ([]os.FileInfo, error)
	Create(string) (File, error)
	Open(string) (io.ReadCloser, error)
	Remove(string) error
	RemoveAll() error
	LastAccess(string) (time.Time, error)
}

// File wraps the underlying WriteCloser source.
type File interface {
	Name() string
	io.WriteCloser
}

type stdFs struct {
	root string
}

// NewFs returns a FileSystem rooted at directory dir
// dir is created with perms if it doesn't exist.
func NewFs(dir string, mode os.FileMode) (FileSystem, error) {
	return &stdFs{root: dir}, os.MkdirAll(dir, mode)
}

func (fs *stdFs) Files() ([]os.FileInfo, error) {
	return ioutil.ReadDir(fs.root)
}

func (fs *stdFs) Create(name string) (File, error) {
	return os.OpenFile(filepath.Join(fs.root, makeName(name)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func (fs *stdFs) Open(name string) (io.ReadCloser, error) {
	return os.Open(name)
}

func (fs *stdFs) Remove(name string) error {
	return os.Remove(name)
}

func (fs *stdFs) RemoveAll() error {
	return os.RemoveAll(fs.root)
}

func (fs *stdFs) LastAccess(name string) (t time.Time, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return t, err
	}
	return atime(fi), nil
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
