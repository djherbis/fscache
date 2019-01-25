package fscache

import (
	"math"
	"time"
)

type CacheStats interface {
	FileSystem() FileSystem
	EnumerateFiles(enumerator func(key string, f FileStream) bool)
	RemoveFile(key string)
}

type Haunter interface {
	Haunt(c CacheStats)
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

// NewCompoundHaunter returns a compound scheduleHaunt which provides a multi strategy implementation
func NewCompoundHaunter(haunters []Haunter) Haunter {
	return &compoundHaunter{
		haunters: haunters,
	}
}

func (h *compoundHaunter) Haunt(c CacheStats) {
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

// NewJanitorHaunter returns a simple scheduleHaunt which provides an implementation Janitor strategy
func NewJanitorHaunter(janitor Janitor) Haunter {
	return &janitorHaunter{
		janitor: janitor,
	}
}

func (h *janitorHaunter) Haunt(c CacheStats) {
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

func (h *reaperHaunter) Haunt(c CacheStats) {
	c.EnumerateFiles(func(key string, f FileStream) bool {
		if f.InUse() {
			return true
		}

		lastRead, lastWrite, err := c.FileSystem().AccessTimes(f.Name())
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
