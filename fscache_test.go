package fscache

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func TestLoadCleanup1(t *testing.T) {
	os.Mkdir("./cache6", 0700)
	f, err := createFile(filepath.Join("./cache6", "s11111111"+tob64("test")))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()
	<-time.After(time.Second)
	f, err = createFile(filepath.Join("./cache6", "s22222222"+tob64("test")))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()

	c, err := New("./cache6", 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	if !c.Exists("test") {
		t.Errorf("expected test to exist")
	}
}

const longString = `
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
	0123456789 0123456789
`

func TestLoadCleanup2(t *testing.T) {
	hash := md5.Sum([]byte(longString))
	name2 := fmt.Sprintf("%s%s%x", longPrefix, "22222222", hash[:])
	name1 := fmt.Sprintf("%s%s%x", longPrefix, "11111111", hash[:])

	os.Mkdir("./cache7", 0700)
	f, err := createFile(filepath.Join("./cache7", name2))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()
	f, err = createFile(filepath.Join("./cache7", fmt.Sprintf("%s.key", name2)))
	if err != nil {
		t.Error(err.Error())
	}
	f.Write([]byte(longString))
	f.Close()
	<-time.After(time.Second)
	f, err = createFile(filepath.Join("./cache7", name1))
	if err != nil {
		t.Error(err.Error())
	}
	f.Close()
	f, err = createFile(filepath.Join("./cache7", fmt.Sprintf("%s.key", name1)))
	if err != nil {
		t.Error(err.Error())
	}
	f.Write([]byte(longString))
	f.Close()

	c, err := New("./cache7", 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	if !c.Exists(longString) {
		t.Errorf("expected test to exist")
	}
}

func TestReload(t *testing.T) {
	c, err := New("./cache5", 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	r, w, err := c.Get("stream")
	if err != nil {
		t.Error(err.Error())
		return
	}
	r.Close()
	w.Write([]byte("hello world\n"))
	w.Close()

	nc, err := New("./cache5", 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer nc.Clean()

	if !nc.Exists("stream") {
		t.Errorf("expected stream to be reloaded")
	} else {
		nc.Remove("stream")
		if nc.Exists("stream") {
			t.Errorf("expected stream to be removed")
		}
	}
}

func TestReaper(t *testing.T) {
	fs, err := NewFs("./cache1", 0700)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	c, err := NewCache(fs, NewReaper(0*time.Second, 100*time.Millisecond))

	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	r, w, err := c.Get("stream")
	w.Write([]byte("hello"))
	w.Close()
	io.Copy(ioutil.Discard, r)

	if !c.Exists("stream") {
		t.Errorf("stream should exist")
	}

	<-time.After(200 * time.Millisecond)

	if !c.Exists("stream") {
		t.Errorf("a file expired while in use, fail!")
	}
	r.Close()

	<-time.After(200 * time.Millisecond)

	if c.Exists("stream") {
		t.Errorf("stream should have been reaped")
	}

	files, err := ioutil.ReadDir("./cache1")
	if err != nil {
		t.Error(err.Error())
		return
	}

	if len(files) > 0 {
		t.Errorf("expected empty directory")
	}
}

func TestReaperNoExpire(t *testing.T) {
	c, err := New("./cache4", 0700, 1)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	r, w, err := c.Get("stream")
	w.Write([]byte("hello"))
	w.Close()
	io.Copy(ioutil.Discard, r)
	r.Close()

	if !c.Exists("stream") {
		t.Errorf("stream should exist")
	}

	c.haunt()
	if !c.Exists("stream") {
		t.Errorf("stream shouldn't have been reaped")
	}
}

func TestSanity(t *testing.T) {
	c, err := New("./cache2", 0700, 1)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	r, w, err := c.Get(longString)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer r.Close()

	w.Write([]byte("hello world\n"))
	w.Close()

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, r)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !bytes.Equal(buf.Bytes(), []byte("hello world\n")) {
		t.Errorf("unexpected output %s", buf.Bytes())
	}
}

func TestConcurrent(t *testing.T) {
	c, err := New("./cache3", 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	r, w, err := c.Get("stream")
	r.Close()
	if err != nil {
		t.Error(err.Error())
		return
	}
	go func() {
		w.Write([]byte("hello"))
		<-time.After(100 * time.Millisecond)
		w.Write([]byte("world"))
		w.Close()
	}()

	if c.Exists("stream") {
		r, _, err := c.Get("stream")
		if err != nil {
			t.Error(err.Error())
			return
		}
		buf := bytes.NewBuffer(nil)
		io.Copy(buf, r)
		r.Close()
		if !bytes.Equal(buf.Bytes(), []byte("helloworld")) {
			t.Errorf("unexpected output %s", buf.Bytes())
		}
	}
}
