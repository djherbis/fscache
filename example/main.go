package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/djherbis/fscache"
)

func connect() fscache.Cache {
	var caches []fscache.Cache
	for i := 0; i < 3; i++ {
		caches = append(caches, fscache.NewRemote(fmt.Sprintf("localhost:%d", 10000+i)))
	}
	return fscache.NewPartition(fscache.NewDistributor(caches...))
}

func main() {
	if len(os.Args) >= 2 {
		for i := 0; i < 3; i++ {
			mc, _ := fscache.NewCache(fscache.NewMemFs(), nil)
			go fscache.ListenAndServe(mc, fmt.Sprintf("localhost:%d", 10000+i))
		}
		<-time.After(time.Hour)
	} else {

		c := connect()

		r, w, err := c.Get("test")
		if err != nil {
			log.Fatal(err.Error())
		}

		if w != nil {
			fmt.Println("MISS")
			w.Write([]byte("hello world :)\n"))
			w.Close()
		} else {
			fmt.Println("HIT")
		}

		io.Copy(os.Stdout, r)
		r.Close()

		if c.Exists("test") {
			fmt.Println("yes!")
		}

		if !c.Exists("jfkdskfl") {
			fmt.Println("nooo!")
		}
	}
}

func xmain() {
	c, _ := fscache.NewCache(fscache.NewMemFs(), fscache.NewReaper(time.Second, time.Second))
	c2, _ := fscache.NewCache(fscache.NewMemFs(), fscache.NewReaper(time.Second, time.Second))
	c3, _ := fscache.NewCache(fscache.NewMemFs(), nil)

	lc := fscache.NewLayered(c, c2, c3)

	r, w, err := lc.Get("stream")
	if err != nil {
		log.Fatal(err.Error())
	}

	w.Write([]byte("hello world\n"))
	w.Close()

	io.Copy(os.Stdout, r)
	r.Close()

	r, w, err = lc.Get("stream")
	if err != nil {
		log.Fatal(err.Error())
	}

	io.Copy(os.Stdout, r)
	r.Close()

	for {
		<-time.After(2 * time.Second)

		r, w, err = lc.Get("stream")
		if err != nil {
			log.Fatal(err.Error())
		}

		io.Copy(os.Stdout, r)
		r.Close()

		if w != nil {
			log.Fatal("problem")
		}

		r, w, err = lc.Get("stream")
		if err != nil {
			log.Fatal(err.Error())
		}

		io.Copy(os.Stdout, r)
		r.Close()
	}

}
