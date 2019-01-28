package fscache

import (
	"time"
)

type Entry interface {
	InUse() bool
	Name() string
}

type CacheAccessor interface {
	FileSystemStater
	EnumerateEntries(enumerator func(key string, e Entry) bool)
	RemoveFile(key string)
}

type Haunter interface {
	Haunt(c CacheAccessor)
	Next() time.Duration
}

type reaperHaunter struct {
	reaper Reaper
}

type janitorHaunter struct {
	janitor Janitor
}

// NewJanitorHaunter returns a simple scheduleHaunt which provides an implementation Janitor strategy
func NewJanitorHaunter(janitor Janitor) Haunter {
	return &janitorHaunter{
		janitor: janitor,
	}
}

func (h *janitorHaunter) Haunt(c CacheAccessor) {
	for _, key := range h.janitor.Scrub(c) {
		c.RemoveFile(key)
	}

}

func (h *janitorHaunter) Next() time.Duration {
	return h.janitor.Next()
}

// NewReaperHaunter returns a simple scheduleHaunt which provides an implementation Reaper strategy
func NewReaperHaunter(reaper Reaper) Haunter {
	return &reaperHaunter{
		reaper: reaper,
	}
}

func (h *reaperHaunter) Haunt(c CacheAccessor) {
	c.EnumerateEntries(func(key string, e Entry) bool {
		if e.InUse() {
			return true
		}

		lastRead, lastWrite, err := c.AccessTimes(e.Name())
		if err != nil {
			return true
		}

		if h.reaper.Reap(key, lastRead, lastWrite) {
			c.RemoveFile(key)
		}

		return true
	})
}

func (h *reaperHaunter) Next() time.Duration {
	return h.reaper.Next()
}
