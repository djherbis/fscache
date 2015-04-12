package fscache

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"io"
)

// Distributor is a function which given a key returns an i from 0 to n-1.
type Distributor func(key string, n uint64) (i uint64)

// StdDistributor distributes the keyspace evenly.
func StdDistributor(key string, n uint64) uint64 {
	h := sha1.New()
	io.WriteString(h, key)
	buf := bytes.NewBuffer(h.Sum(nil)[:8])
	i, _ := binary.ReadUvarint(buf)
	return i % n
}

type distrib struct {
	distributor Distributor
	caches      []Cache
	size        uint64
}

// NewDistributed returns a cache which shards across the passed caches evenly.
func NewDistributed(caches ...Cache) Cache {
	return NewCustomDistributed(StdDistributor, caches...)
}

// NewCustomDistributed returns a cache which shards according to Distributor d.
func NewCustomDistributed(d Distributor, caches ...Cache) Cache {
	if len(caches) == 0 {
		return nil
	}
	return &distrib{
		distributor: d,
		caches:      caches,
		size:        uint64(len(caches)),
	}
}

func (d *distrib) getCache(key string) Cache {
	return d.caches[d.distributor(key, d.size)]
}

func (d *distrib) Get(key string) (io.ReadCloser, io.WriteCloser, error) {
	return d.getCache(key).Get(key)
}

func (d *distrib) Remove(key string) error {
	return d.getCache(key).Remove(key)
}

func (d *distrib) Exists(key string) bool {
	return d.getCache(key).Exists(key)
}

// BUG(djherbis): Return an error if cleaning fails
func (d *distrib) Clean() error {
	for _, c := range d.caches {
		c.Clean()
	}
	return nil
}
