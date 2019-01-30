package fscache

import (
	"os"
	"time"
)

type FileInfo struct {
	name     string
	size     int64
	fileMode os.FileMode
	isDir    bool
	sys      interface{}
	rt       time.Time
	wt       time.Time
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Size() int64 {
	return f.size
}

func (f *FileInfo) Mode() os.FileMode {
	return f.fileMode
}

func (f *FileInfo) ModTime() time.Time {
	return f.wt
}

func (f *FileInfo) IsDir() bool {
	return f.isDir
}

func (f *FileInfo) Sys() interface{} {
	return f.sys
}

// AccessTimes returns the last time the file was read,
// and the last time it was written to.
// It will be used to check expiry of a file, and must be concurrent safe
// with modifications to the FileSystem (writes, reads etc.)
func (f *FileInfo) AccessTimes() (rt, wt time.Time) {
	return f.rt, f.wt
}
