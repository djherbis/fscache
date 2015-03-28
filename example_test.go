package fscache

import (
	"io"
	"log"
	"os"
)

func Example() {
	// create the cache, keys expire after 1 hour.
	c, err := New("./cache", 0755, 1)
	if err != nil {
		log.Fatal(err.Error())
	}

	// wipe the cache when done
	defer c.Clean()

	// Get() and it's streams can be called concurrently but just for example:
	for i := 0; i < 3; i++ {
		r, w, err := c.Get("stream")
		if err != nil {
			log.Fatal(err.Error())
		}

		if w != nil { // a new stream, write to it.
			go func() {
				w.Write([]byte("hello world\n"))
				w.Close()
			}()
		}

		// the stream has started, read from it
		io.Copy(os.Stdout, r)
		r.Close()
	}
	// Output:
	// hello world
	// hello world
	// hello world
}
