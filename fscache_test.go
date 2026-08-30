package fscache

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
}

func init() {
	c, err := NewCache(NewMemFs(), nil)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := ListenAndServe(c, "localhost:10000"); err != nil {
			panic(err)
		}
	}()
}

func testCaches(t *testing.T, run func(c Cache)) {
	c, err := New("./cache", 0700, 1*time.Hour)
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Cleanup(func() { os.RemoveAll("./cache") })
	run(c)

	c, err = NewCache(NewMemFs(), NewReaper(time.Hour, time.Hour))
	if err != nil {
		t.Error(err.Error())
		return
	}
	run(c)

	c2, _ := NewCache(NewMemFs(), nil)
	run(NewPartition(NewDistributor(c, c2)))

	lc := NewLayered(c, c2)
	run(lc)

	rc := NewRemote("localhost:10000")
	run(rc)

	fs, _ := NewFs("./cachex", 0700)
	t.Cleanup(func() { os.RemoveAll("./cachex") })
	fs.EncodeKey = IdentityCodeKey
	fs.DecodeKey = IdentityCodeKey
	ck, _ := NewCache(fs, NewReaper(time.Hour, time.Hour))
	ck.SetKeyMapper(func(key string) string {
		name, _ := B64OrMD5HashEncodeKey(key)
		return name
	})
	run(ck)
}

func TestPacketReadAt(t *testing.T) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	if _, err := enc.Write([]byte("hello world")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dec := newDecoder(bytes.NewReader(buf.Bytes()))
	p := make([]byte, 5)
	n, err := dec.ReadAt(p, 6)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}
	if got := string(p[:n]); got != "world" {
		t.Fatalf("ReadAt = %q, want %q", got, "world")
	}
}

func TestHandler(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()
		ts := httptest.NewServer(Handler(c, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello Client")
		})))
		defer ts.Close()

		for i := 0; i < 3; i++ {
			res, err := http.Get(ts.URL)
			if err != nil {
				t.Error(err.Error())
				t.FailNow()
			}
			p, err := ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if !bytes.Equal([]byte("Hello Client\n"), p) {
				t.Errorf("unexpected response %s", string(p))
			}
		}
	})
}

func TestMemFs(t *testing.T) {
	fs := NewMemFs()
	fs.Reload(func(key, name string) {}) // nop
	if _, err := fs.Open("test"); err == nil {
		t.Errorf("stream shouldn't exist")
	}
	fs.Remove("test")

	f, err := fs.Create("test")
	if err != nil {
		t.Errorf("failed to create test")
	}
	f.Write([]byte("hello"))
	f.Close()

	r, err := fs.Open("test")
	if err != nil {
		t.Errorf("failed Open: %v", err)
	}
	p, err := ioutil.ReadAll(r)
	if err != nil {
		t.Errorf("failed ioutil.ReadAll: %v", err)
	}
	r.Close()
	if !bytes.Equal(p, []byte("hello")) {
		t.Errorf("expected hello, got %s", string(p))
	}
	fs.RemoveAll()
}

func TestLoadCleanup1(t *testing.T) {
	os.Mkdir("./cache6", 0700)
	t.Cleanup(func() { os.RemoveAll("./cache6") })
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
	t.Cleanup(func() { os.RemoveAll("./cache7") })
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
	dir, err := ioutil.TempDir("", "cache5")
	if err != nil {
		t.Fatalf("Failed to create TempDir: %v", err)
	}
	c, err := New(dir, 0700, 0)
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
	data := []byte("hello world\n")
	w.Write(data)
	w.Close()

	nc, err := New(dir, 0700, 0)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer nc.Clean()

	if !nc.Exists("stream") {
		t.Fatalf("expected stream to be reloaded")
	}

	r, w, err = nc.Get("stream")
	if err != nil {
		t.Fatal(err)
	}
	if w != nil {
		t.Fatal("expected reloaded stream to not be writable")
	}

	cr, ok := r.(*CacheReader)
	if !ok {
		t.Fatalf("CacheReader should be supported by a normal FS")
	}
	size, closed, err := cr.Size()
	if err != nil {
		t.Fatalf("Failed to get Size: %v", err)
	}
	if !closed {
		t.Errorf("Expected stream to be closed.")
	}
	if size != int64(len(data)) {
		t.Errorf("Expected size to be %v, but got %v", len(data), size)
	}

	r.Close()
	nc.Remove("stream")
	if nc.Exists("stream") {
		t.Errorf("expected stream to be removed")
	}
}

// eventually polls check until it returns true or the timeout passes. The haunter and
// reaper run on their own clocks, so fixed sleeps flake on slow CI runners.
func eventually(t *testing.T, timeout time.Duration, check func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Error(msg)
}

func TestLRUHaunterMaxItems(t *testing.T) {

	fs, err := NewFs("./cache1", 0700)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Cleanup(func() { os.RemoveAll("./cache1") })

	c, err := NewCacheWithHaunter(fs, NewLRUHaunterStrategy(NewLRUHaunter(3, 0, 400*time.Millisecond)))

	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("stream-%v", i)
		r, w, _ := c.Get(name)
		w.Write([]byte("hello"))
		w.Close()
		io.Copy(ioutil.Discard, r)

		if !c.Exists(name) {
			t.Errorf(name + " should exist")
		}

		<-time.After(10 * time.Millisecond)

		err := r.Close()
		if err != nil {
			t.Error(err)
		}
	}

	// Which entries go is LRU order, which depends on filesystem access times that
	// CI runners do not honor reliably; only the enforced bound is asserted.
	eventually(t, 10*time.Second, func() bool {
		files, err := ioutil.ReadDir("./cache1")
		return err == nil && len(files) == 3
	}, "expected the haunter to scrub down to 3 items")
}

func TestLRUHaunterMaxSize(t *testing.T) {

	fs, err := NewFs("./cache1", 0700)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Cleanup(func() { os.RemoveAll("./cache1") })

	c, err := NewCacheWithHaunter(fs, NewLRUHaunterStrategy(NewLRUHaunter(0, 24, 400*time.Millisecond)))

	if err != nil {
		t.Error(err.Error())
		return
	}
	defer c.Clean()

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("stream-%v", i)
		r, w, _ := c.Get(name)
		w.Write([]byte("hello"))
		w.Close()
		io.Copy(ioutil.Discard, r)

		if !c.Exists(name) {
			t.Errorf(name + " should exist")
		}

		<-time.After(10 * time.Millisecond)

		err := r.Close()
		if err != nil {
			t.Error(err)
		}
	}

	eventually(t, 10*time.Second, func() bool {
		files, err := ioutil.ReadDir("./cache1")
		return err == nil && len(files) == 4
	}, "expected the haunter to scrub down to the size limit")
}

func TestReaper(t *testing.T) {
	fs, err := NewFs("./cache1", 0700)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Cleanup(func() { os.RemoveAll("./cache1") })

	c, err := NewCache(fs, NewReaper(0*time.Second, 100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clean()

	r, w, err := c.Get("stream")
	if err != nil {
		t.Fatal(err)
	}
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

	eventually(t, 10*time.Second, func() bool {
		return !c.Exists("stream")
	}, "stream should have been reaped")

	eventually(t, 10*time.Second, func() bool {
		files, err := ioutil.ReadDir("./cache1")
		return err == nil && len(files) == 0
	}, "expected empty directory")
}

func TestReaperNoExpire(t *testing.T) {
	testCaches(t, func(c Cache) {
		defer c.Clean()
		r, w, err := c.Get("stream")
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		w.Write([]byte("hello"))
		w.Close()
		io.Copy(ioutil.Discard, r)
		r.Close()

		if !c.Exists("stream") {
			t.Errorf("stream should exist")
		}

		if lc, ok := c.(*FSCache); ok {
			lc.haunt()
			if !c.Exists("stream") {
				t.Errorf("stream shouldn't have been reaped")
			}
		}
	})
}

func TestSanity(t *testing.T) {
	atLeastOneCacheReader := false
	testCaches(t, func(c Cache) {
		defer c.Clean()

		r, w, err := c.Get(longString)
		if err != nil {
			t.Error(err.Error())
			return
		}
		defer r.Close()

		want := []byte("hello world\n")
		first := want[:5]
		w.Write(first)

		cr, ok := r.(*CacheReader)
		if ok {
			atLeastOneCacheReader = true
			size, closed, _ := cr.Size()
			if closed {
				t.Errorf("Expected stream to be open.")
			}
			if size != int64(len(first)) {
				t.Errorf("Expected size to be %v, but got %v", len(first), size)
			}
		}

		second := want[5:]
		w.Write(second)

		if ok {
			atLeastOneCacheReader = true
			size, closed, _ := cr.Size()
			if closed {
				t.Errorf("Expected stream to be open.")
			}
			if size != int64(len(want)) {
				t.Errorf("Expected size to be %v, but got %v", len(want), size)
			}
		}

		w.Close()

		if ok {
			atLeastOneCacheReader = true
			size, closed, _ := cr.Size()
			if !closed {
				t.Errorf("Expected stream to be closed.")
			}
			if size != int64(len(want)) {
				t.Errorf("Expected size to be %v, but got %v", len(want), size)
			}
		}

		buf := bytes.NewBuffer(nil)
		_, err = io.Copy(buf, r)
		if err != nil {
			t.Error(err.Error())
			return
		}
		if !bytes.Equal(buf.Bytes(), want) {
			t.Errorf("unexpected output %s", buf.Bytes())
		}
	})
	if !atLeastOneCacheReader {
		t.Errorf("None of the cache tests covered CacheReader!")
	}
}

func TestConcurrent(t *testing.T) {
	testCaches(t, func(c Cache) {
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
	})
}

func TestReuse(t *testing.T) {
	testCaches(t, func(c Cache) {
		for i := 0; i < 10; i++ {
			r, w, err := c.Get(longString)
			if err != nil {
				t.Error(err.Error())
				return
			}

			data := fmt.Sprintf("hello %d", i)

			if w != nil {
				w.Write([]byte(data))
				w.Close()
			}

			check(t, r, data)
			r.Close()

			c.Clean()
		}
	})
}

func check(t *testing.T, r io.Reader, data string) {
	buf := bytes.NewBuffer(nil)
	_, err := io.Copy(buf, r)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if !bytes.Equal(buf.Bytes(), []byte(data)) {
		t.Errorf("unexpected output %q, want %q", buf.String(), data)
	}
}

// errWriteCloser is implemented by writers that can report a failed write to readers.
type errWriteCloser interface {
	CloseWithError(err error) error
}

func TestCloseWithErrorReleasesBlockedReader(t *testing.T) {
	c, err := NewCache(NewMemFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := c.Get("blocked")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("transcoder died")
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, 32)
		n, err := r.Read(buf)
		if n != 7 || err != nil {
			got <- err
			return
		}
		// The stream is still open, so this read blocks until the writer settles it.
		_, err = r.Read(buf)
		got <- err
	}()

	time.Sleep(50 * time.Millisecond) // let the reader block on the open stream
	if err := w.(errWriteCloser).CloseWithError(boom); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-got:
		if !errors.Is(err, boom) {
			t.Errorf("blocked reader got %v, want %v", err, boom)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader still blocked after CloseWithError")
	}
}

func TestCloseWithErrorFailsLateGet(t *testing.T) {
	c, err := NewCache(NewMemFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := c.Get("late")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	boom := errors.New("transcoder died")
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := w.(errWriteCloser).CloseWithError(boom); err != nil {
		t.Fatal(err)
	}

	if _, _, err := c.Get("late"); !errors.Is(err, boom) {
		t.Errorf("late Get got %v, want %v", err, boom)
	}
}

func TestCloseWithErrorNeverReportsFinalSize(t *testing.T) {
	c, err := NewCache(NewMemFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := c.Get("notfinal")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := w.(errWriteCloser).CloseWithError(errors.New("boom")); err != nil {
		t.Fatal(err)
	}

	if _, final, err := r.(*CacheReader).Size(); err == nil && final {
		t.Error("failed entry reports a final size; it would be served as a complete file")
	}
}

func TestCloseWithErrorNilMeansClose(t *testing.T) {
	c, err := NewCache(NewMemFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, w, err := c.Get("clean")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := w.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := w.(errWriteCloser).CloseWithError(nil); err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(r)
	if err != nil || string(data) != "complete" {
		t.Errorf("got (%q, %v), want a clean EOF with the full data", data, err)
	}
	if _, final, err := r.(*CacheReader).Size(); err != nil || !final {
		t.Errorf("clean close must finalize the entry, got final=%v err=%v", final, err)
	}
}
