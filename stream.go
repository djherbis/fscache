package fscache

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
)

type decoder interface {
	Decode(interface{}) error
}

type encoder interface {
	Encode(interface{}) error
}

type pktReader struct {
	dec decoder

	mu   sync.Mutex
	buf  []byte
	pos  int64
	eof  bool
}

type pktWriter struct {
	enc encoder
}

type packet struct {
	Err  int
	Data []byte
}

const eof = 1

func (t *pktReader) readAhead(goal int64) error {
	for int64(len(t.buf)) < goal && !t.eof {
		var pkt packet
		if err := t.dec.Decode(&pkt); err != nil {
			if err == io.EOF {
				t.eof = true
				return nil
			}
			return err
		}
		if pkt.Err == eof {
			t.eof = true
			return nil
		}
		t.buf = append(t.buf, pkt.Data...)
	}
	return nil
}

func (t *pktReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.readAhead(off + int64(len(p))); err != nil {
		return 0, err
	}
	if off >= int64(len(t.buf)) {
		return 0, io.EOF
	}

	n = copy(p, t.buf[off:])
	if n < len(p) && t.eof {
		return n, io.EOF
	}
	return n, nil
}

func (t *pktReader) Read(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.readAhead(t.pos + int64(len(p))); err != nil {
		return 0, err
	}
	if t.pos >= int64(len(t.buf)) {
		return 0, io.EOF
	}

	n := copy(p, t.buf[t.pos:])
	t.pos += int64(n)
	if n < len(p) && t.eof {
		return n, io.EOF
	}
	return n, nil
}

func (t *pktReader) Close() error {
	return nil
}

func (t *pktWriter) Write(p []byte) (int, error) {
	pkt := packet{Data: p}
	err := t.enc.Encode(pkt)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (t *pktWriter) Close() error {
	return t.enc.Encode(packet{Err: eof})
}

func newEncoder(w io.Writer) io.WriteCloser {
	return &pktWriter{enc: json.NewEncoder(w)}
}

func newDecoder(r io.Reader) ReadAtCloser {
	return &pktReader{dec: json.NewDecoder(r)}
}
