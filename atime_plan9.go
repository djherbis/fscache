package fscache

import (
	"os"
	"time"
)

func atime(fi os.FileInfo) time.Time {
	return time.Unix(int64(fi.Sys().(*syscall.Dir).Atime), 0)
}
