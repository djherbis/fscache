package fscache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Handler is a caching middle-ware for http Handlers.
// It responds to http requests via the passed http.Handler, and caches the response
// using the passed cache. The cache key for the request is the req.URL.String().
func Handler(c Cache, h http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		url := req.URL.String()
		r, w, err := c.Get(url)
		if err != nil {
			h.ServeHTTP(rw, req)
			return
		}
		defer r.Close()
		if w != nil {
			go func() {
				defer w.Close()
				h.ServeHTTP(&respWrapper{
					ResponseWriter: rw,
					Writer:         w,
				}, req)
			}()
		}
		decodeHeader(r, rw.Header())
		io.Copy(rw, r)
	})
}

func encodeHeader(w io.Writer, h http.Header) error {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(h)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, int64(buf.Len()))
	if err != nil {
		return err
	}
	_, err = io.Copy(w, buf)
	return err
}

func decodeHeader(r io.Reader, h http.Header) error {
	var size int64
	_, err := fmt.Fscanln(r, &size)
	if err != nil {
		return err
	}
	dec := gob.NewDecoder(io.LimitReader(r, size))
	return dec.Decode(&h)
}

type respWrapper struct {
	http.ResponseWriter
	io.Writer
	sync.Once
}

func (r *respWrapper) Write(p []byte) (int, error) {
	r.Do(func() { encodeHeader(r.Writer, r.Header()) })
	return r.Writer.Write(p)
}
