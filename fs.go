package fscache

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/djherbis/atime.v1"
	"gopkg.in/djherbis/stream.v1"
)

type FileSystemStater interface {
	// Stat takes a File.Name() and returns FileInfo interface
	Stat(name string) (FileInfo, error)
}

// FileSystem is used as the source for a Cache.
type FileSystem interface {
	// Stream FileSystem
	stream.FileSystem

	FileSystemStater

	// Reload should look through the FileSystem and call the supplied fn
	// with the key/filename pairs that are found.
	Reload(func(key, name string)) error

	// RemoveAll should empty the FileSystem of all files.
	RemoveAll() error
}

type stdFs struct {
	root string
	init func() error
}

// NewFs returns a FileSystem rooted at directory dir.
// Dir is created with perms if it doesn't exist.
func NewFs(dir string, mode os.FileMode) (FileSystem, error) {
	fs := &stdFs{root: dir, init: func() error {
		return os.MkdirAll(dir, mode)
	}}
	return fs, fs.init()
}

func (fs *stdFs) Reload(add func(key, name string)) error {
	files, err := ioutil.ReadDir(fs.root)
	if err != nil {
		return err
	}

	addfiles := make(map[string]struct {
		os.FileInfo
		key string
	})

	for _, f := range files {

		if strings.HasSuffix(f.Name(), ".key") {
			continue
		}

		key, err := fs.getKey(f.Name())
		if err != nil {
			fs.Remove(filepath.Join(fs.root, f.Name()))
			continue
		}
		fi, ok := addfiles[key]

		if !ok || fi.ModTime().Before(f.ModTime()) {
			if ok {
				fs.Remove(fi.Name())
			}
			addfiles[key] = struct {
				os.FileInfo
				key string
			}{
				FileInfo: f,
				key:      key,
			}
		} else {
			fs.Remove(f.Name())
		}

	}

	for _, f := range addfiles {
		path, err := filepath.Abs(filepath.Join(fs.root, f.Name()))
		if err != nil {
			return err
		}
		add(f.key, path)
	}

	return nil
}

func (fs *stdFs) Create(name string) (stream.File, error) {
	name, err := fs.makeName(name)
	if err != nil {
		return nil, err
	}
	return fs.create(name)
}

func (fs *stdFs) create(name string) (stream.File, error) {
	return os.OpenFile(filepath.Join(fs.root, name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func (fs *stdFs) Open(name string) (stream.File, error) {
	return os.Open(name)
}

func (fs *stdFs) Remove(name string) error {
	os.Remove(fmt.Sprintf("%s.key", name))
	return os.Remove(name)
}

func (fs *stdFs) RemoveAll() error {
	if err := os.RemoveAll(fs.root); err != nil {
		return err
	}
	return fs.init()
}

func (fs *stdFs) AccessTimes(name string) (rt, wt time.Time, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		return rt, wt, err
	}
	return atime.Get(fi), fi.ModTime(), nil
}

func (fs *stdFs) Stat(name string) (FileInfo, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{FileInfo: stat, Atime: atime.Get(stat)}, nil
}

const (
	saltSize    = 8
	maxShort    = 20
	shortPrefix = "s"
	longPrefix  = "l"
)

func salt() string {
	buf := bytes.NewBufferString("")
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	io.CopyN(enc, rand.Reader, saltSize)
	return buf.String()
}

func tob64(s string) string {
	buf := bytes.NewBufferString("")
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	enc.Write([]byte(s))
	enc.Close()
	return buf.String()
}

func fromb64(s string) string {
	buf := bytes.NewBufferString(s)
	dec := base64.NewDecoder(base64.URLEncoding, buf)
	out := bytes.NewBufferString("")
	io.Copy(out, dec)
	return out.String()
}

func (fs *stdFs) makeName(key string) (string, error) {
	b64key := tob64(key)
	// short name
	if len(b64key) < maxShort {
		return fmt.Sprintf("%s%s%s", shortPrefix, salt(), b64key), nil
	}

	// long name
	hash := md5.Sum([]byte(key))
	name := fmt.Sprintf("%s%s%x", longPrefix, salt(), hash[:])
	f, err := fs.create(fmt.Sprintf("%s.key", name))
	if err != nil {
		return "", err
	}
	_, err = f.Write([]byte(key))
	f.Close()
	return name, err
}

func (fs *stdFs) getKey(name string) (string, error) {
	// short name
	if strings.HasPrefix(name, shortPrefix) {
		return fromb64(strings.TrimPrefix(name, shortPrefix)[saltSize:]), nil
	}

	// long name
	f, err := fs.Open(filepath.Join(fs.root, fmt.Sprintf("%s.key", name)))
	if err != nil {
		return "", err
	}
	defer f.Close()
	key, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(key), nil
}
