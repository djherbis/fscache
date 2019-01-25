package fscache

import (
	"math"
	"time"
)

type Haunter interface {
	Haunt(c *cache)
	Next() time.Duration
}

type reaperHaunter struct {
	reaper Reaper
}

type janitorHaunter struct {
	janitor Janitor
}

type compoundHaunter struct {
	haunters []Haunter
}

// NewCompoundHaunter returns a compound haunter which provides a multi strategy implementation
func NewCompoundHaunter(haunters []Haunter) Haunter {
	return &compoundHaunter{
		haunters: haunters,
	}
}

func (h *compoundHaunter) Haunt(c *cache) {
	for _, haunter := range h.haunters {
		haunter.Haunt(c)
	}
}

func (h *compoundHaunter) Next() time.Duration {
	minPeriod := time.Duration(math.MaxInt64)
	for _, haunter := range h.haunters {
		if period := haunter.Next(); period < minPeriod {
			minPeriod = period
		}
	}

	return minPeriod
}

// NewJanitorHaunter returns a simple haunter which provides an implementation Janitor strategy
func NewJanitorHaunter(janitor Janitor) Haunter {
	return &janitorHaunter{
		janitor: janitor,
	}
}

func (h *janitorHaunter) Haunt(c *cache) {
	for _, key := range h.janitor.Scrub(c) {
		f, ok := c.files[key]
		delete(c.files, key)
		if ok {
			c.fs.Remove(f.Name())
		}
	}

}

func (h *janitorHaunter) Next() time.Duration {
	return h.janitor.Next()
}

// NewReaperHaunter returns a simple haunter which provides an implementation Reaper strategy
func NewReaperHaunter(reaper Reaper) Haunter {
	return &reaperHaunter{
		reaper: reaper,
	}
}

func (h *reaperHaunter) Haunt(c *cache) {
	for key, f := range c.files {
		if f.inUse() {
			continue
		}

		lastRead, lastWrite, err := c.fs.AccessTimes(f.Name())
		if err != nil {
			continue
		}

		if h.reaper.Reap(key, lastRead, lastWrite) {
			delete(c.files, key)
			c.fs.Remove(f.Name())
		}
	}
}

func (h *reaperHaunter) Next() time.Duration {
	return h.reaper.Next()
}
