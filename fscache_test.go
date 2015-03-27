package fscache

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestSanity(t *testing.T) {
	c, err := New("./cache", 0700, 0)
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
	c, err := New("./cache", 0700, 0)
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
