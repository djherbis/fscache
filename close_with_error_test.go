package fscache

import (
	"errors"
	"io"
	"testing"
	"time"
)

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
