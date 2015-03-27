package fscache

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

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
	c, err := New("./cache1", 0700, 0)
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

	c.reap()
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

	c.reap()
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

	r, w, err := c.Get("stream")
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
		defer r.Close()
		if err != nil {
			t.Error(err.Error())
			return
		}
		buf := bytes.NewBuffer(nil)
		io.Copy(buf, r)
		if !bytes.Equal(buf.Bytes(), []byte("helloworld")) {
			t.Errorf("unexpected output %s", buf.Bytes())
		}
	}
}
