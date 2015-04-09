package fscache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
)

// ListenAndServe hosts a Cache for access via NewRemote
func ListenAndServe(c Cache, addr string) error {
	return (&server{c: c}).ListenAndServe(addr)
}

// NewRemote returns a Cache run via ListenAndServe
func NewRemote(raddr string) Cache {
	return &remote{raddr: raddr}
}

type server struct {
	c Cache
}

func (s *server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}

		go s.Serve(c)
	}
}

const (
	actionGet    = iota
	actionRemove = iota
	actionExists = iota
	actionClean  = iota
)

func getKey(r io.Reader) string {
	dec := newDecoder(r)
	buf := bytes.NewBufferString("")
	io.Copy(buf, dec)
	return buf.String()
}

func sendKey(w io.Writer, key string) {
	enc := newEncoder(w)
	enc.Write([]byte(key))
	enc.Close()
}

func (s *server) Serve(c net.Conn) {
	defer c.Close() // BUG(djherbis) wrong side hang-up

	var action int
	fmt.Fscanf(c, "%d\n", &action)

	switch action {
	case actionGet:
		s.get(c, getKey(c))
	case actionRemove:
		s.c.Remove(getKey(c))
	case actionExists:
		s.exists(c, getKey(c))
	case actionClean:
		s.c.Clean()
	}
}

func (s *server) exists(c net.Conn, key string) {
	if s.c.Exists(key) {
		fmt.Fprintf(c, "%d\n", 1)
	} else {
		fmt.Fprintf(c, "%d\n", 0)
	}
}

func (s *server) get(c net.Conn, key string) {
	r, w, err := s.c.Get(key)
	if err != nil {
		return // handle this better
	}
	defer r.Close()

	if w != nil {
		go func() {
			fmt.Fprintf(c, "%d\n", 1)
			io.Copy(w, newDecoder(c))
			w.Close()
		}()
	} else {
		fmt.Fprintf(c, "%d\n", 0)
	}

	enc := newEncoder(c)
	io.Copy(enc, r)
	enc.Close()
}

type remote struct {
	raddr string
}

func (r *remote) Get(key string) (io.ReadCloser, io.WriteCloser, error) {
	c, err := net.Dial("tcp", r.raddr)
	if err != nil {
		return nil, nil, err
	}
	fmt.Fprintf(c, "%d\n", actionGet)
	sendKey(c, key)

	var i int
	fmt.Fscanf(c, "%d\n", &i)

	switch i {
	case 0:
		return newDecoder(c), nil, nil
	case 1:
		return newDecoder(c), newEncoder(c), nil
	default:
		return nil, nil, errors.New("bad bad bad")
	}
}

func (r *remote) Exists(key string) bool {
	c, err := net.Dial("tcp", r.raddr)
	if err != nil {
		return false
	}
	fmt.Fprintf(c, "%d\n", actionExists)
	sendKey(c, key)
	var i int
	fmt.Fscanf(c, "%d\n", &i)
	return i == 1
}

func (r *remote) Remove(key string) error {
	c, err := net.Dial("tcp", r.raddr)
	if err != nil {
		return err
	}
	fmt.Fprintf(c, "%d\n", actionRemove)
	sendKey(c, key)
	return nil
}

func (r *remote) Clean() error {
	c, err := net.Dial("tcp", r.raddr)
	if err != nil {
		return err
	}
	fmt.Fprintf(c, "%d\n", actionClean)
	return nil
}
