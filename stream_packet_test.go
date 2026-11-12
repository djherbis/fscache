package fscache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func remotePacketTestCache(t *testing.T, c Cache) Cache {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		(&server{c: c}).Serve(conn)
	}()
	t.Cleanup(func() {
		listener.Close()
		select {
		case conn := <-accepted:
			conn.Close()
		default:
		}
		<-done
	})
	return NewRemote(listener.Addr().String())
}

func TestRemotePacketContents(t *testing.T) {
	for _, size := range []int{24, 1024, 65553} {
		for _, hit := range []bool{false, true} {
			t.Run(fmt.Sprintf("size=%d/hit=%t", size, hit), func(t *testing.T) {
				cache, err := NewCache(NewMemFs(), nil)
				if err != nil {
					t.Fatal(err)
				}
				payload := bytes.Repeat([]byte("x"), size)
				for i := range payload {
					payload[i] = byte(i % 251)
				}
				if hit {
					r, w, err := cache.Get("packet")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := w.Write(payload); err != nil {
						t.Fatal(err)
					}
					w.Close()
					r.Close()
				}
				remote := remotePacketTestCache(t, cache)
				r, w, err := remote.Get("packet")
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if (w == nil) != hit {
					t.Fatal("unexpected cache hit/miss")
				}
				written := make(chan error, 1)
				if w != nil {
					go func() {
						_, err := w.Write(payload) // one packet, larger than the server's copy buffer
						closeErr := w.Close()
						if err == nil {
							err = closeErr
						}
						written <- err
					}()
				}
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatal(err)
				}
				if w != nil {
					if err := <-written; err != nil {
						t.Fatal(err)
					}
				}
				local, _, err := cache.Get("packet")
				if err != nil {
					t.Fatal(err)
				}
				defer local.Close()
				stored, err := io.ReadAll(local)
				if err != nil || !bytes.Equal(stored, payload) {
					t.Fatalf("stored %d bytes, want %d: %v", len(stored), len(payload), err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("remote returned %d bytes, want all %d", len(got), len(payload))
				}
			})
		}
	}
}

func TestPacketReaderShortReads(t *testing.T) {
	var wire bytes.Buffer
	enc := json.NewEncoder(&wire)
	for _, pkt := range []packet{{Data: []byte("first packet")}, {Data: []byte("second packet")}, {Err: eof}} {
		if err := enc.Encode(pkt); err != nil {
			t.Fatal(err)
		}
	}
	r := newDecoder(&wire)
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty read = (%d, %v)", n, err)
	}
	first := make([]byte, 3)
	if n, err := io.ReadFull(r, first); n != len(first) || err != nil {
		t.Fatalf("short read = (%d, %v)", n, err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(append(first, rest...)); got != "first packetsecond packet" {
		t.Fatalf("decoded %q", got)
	}
}

func TestPacketReaderEmptyPacketsAndErrors(t *testing.T) {
	var wire bytes.Buffer
	enc := json.NewEncoder(&wire)
	for _, pkt := range []packet{{}, {Data: []byte("abc")}} {
		if err := enc.Encode(pkt); err != nil {
			t.Fatal(err)
		}
	}
	wire.WriteString("{invalid json}")
	r := newDecoder(&wire)
	p := make([]byte, 2)
	if n, err := r.Read(p); n != 2 || err != nil || string(p) != "ab" {
		t.Fatalf("first Read = (%d, %v), bytes %q", n, err, p)
	}
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("empty Read = (%d, %v)", n, err)
	}
	if n, err := r.Read(p); n != 1 || err != nil || p[0] != 'c' {
		t.Fatalf("pending Read = (%d, %v), bytes %q", n, err, p[:n])
	}
	if n, err := r.Read(p); n != 0 {
		t.Fatalf("malformed Read returned %d bytes", n)
	} else if _, ok := err.(*json.SyntaxError); !ok {
		t.Fatalf("malformed Read error = %v, want JSON syntax error", err)
	}
}
