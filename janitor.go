package fscache

import (
	"time"
	"os"
	"sort"
	"fmt"
)

type janitorKV struct {
	Key   string
	Value fileStream
}

// Janitor is used to control when there are too many streams
// or the size of the streams is too big.
// It is called once right after loading, and then it is run
// again after every Next() period of time.
type Janitor interface {
	// Returns the amount of time to wait before the next scheduled Reaping.
	Next() time.Duration

	// Given a key and the last r/w times of a file, return true
	// to remove the file from the cache, false to keep it.
	Scrub(c *cache)
}

// NewJanitor returns a simple janitor which runs every "period"
// and scrubs older files when the total file size is over maxSize or
// total item count is over maxItems.
// If maxItems or maxSize are 0, they won't be checked
func NewJanitor(maxItems int, maxSize int64, period time.Duration) Janitor {
	return &janitor{
		period:   period,
		maxItems: maxItems,
		maxSize:  maxSize,
	}
}

type janitor struct {
	period   time.Duration
	maxItems int
	maxSize  int64
}

func (j *janitor) Next() time.Duration {
	return j.period
}

func (j *janitor) Scrub(c *cache) {

	if len(c.files) == 0 {
		return
	}

	var count int
	var size int64
	var okFiles []janitorKV

	c.mu.Lock()

	for k, f := range c.files {
		if f.inUse() {
			continue
		}

		stat, err := os.Stat(f.Name())
		if err != nil {
			continue
		}

		count++
		size = size + stat.Size()
		okFiles = append(okFiles, janitorKV{
			Key:   k,
			Value: f,
		})
	}

	c.mu.Unlock()

	sort.Slice(okFiles, func(i, l int) bool {
		iLastRead, _, err := c.fs.AccessTimes(okFiles[i].Value.Name())
		if err != nil {
			return false
		}

		jLastRead, _, err := c.fs.AccessTimes(okFiles[l].Value.Name())
		if err != nil {
			return false
		}

		return iLastRead.Before(jLastRead)
	})

	if j.maxItems > 0 {
		for count > j.maxItems {
			count, size = j.removeFirst(c, &okFiles, count, size)
		}
	}

	if j.maxSize > 0 {
		for size > j.maxSize {
			count, size = j.removeFirst(c, &okFiles, count, size)
		}
	}

}

func (j *janitor) removeFirst(c *cache, items *[]janitorKV, count int, size int64) (int, int64) {

	var f janitorKV

	f, *items = (*items)[0], (*items)[1:]

	stat, err := os.Stat(f.Value.Name())
	if err != nil {
		fmt.Println(err)
		return count, size
	}

	err = c.Remove(f.Key)
	if err != nil {
		fmt.Println(err)
		return count, size
	}

	count--
	size = size - stat.Size()

	return count, size

}
