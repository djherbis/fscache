package fscache

import (
	"sort"
	"time"
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
	Scrub(c *cache) []string
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

func (j *janitor) Scrub(cache *cache) (keysToReap []string) {
	if len(cache.files) == 0 {
		return
	}

	var count int
	var size int64
	var okFiles []janitorKV

	for k, f := range cache.files {
		if f.inUse() {
			continue
		}

		fileSize, err := cache.fs.Size(f.Name())
		if err != nil {
			continue
		}

		count++
		size = size + fileSize
		okFiles = append(okFiles, janitorKV{
			Key:   k,
			Value: f,
		})
	}

	sort.Slice(okFiles, func(i, l int) bool {
		iLastRead, _, err := cache.fs.AccessTimes(okFiles[i].Value.Name())
		if err != nil {
			return false
		}

		jLastRead, _, err := cache.fs.AccessTimes(okFiles[l].Value.Name())
		if err != nil {
			return false
		}

		return iLastRead.Before(jLastRead)
	})

	if j.maxItems > 0 {
		for count > j.maxItems {
			var key *string
			var err error
			key, count, size, err = j.removeFirst(cache, &okFiles, count, size)
			if err != nil {
				break
			}
			if key != nil {
				keysToReap = append(keysToReap, *key)
			}
		}
	}

	if j.maxSize > 0 {
		for size > j.maxSize {
			var key *string
			var err error
			key, count, size, err = j.removeFirst(cache, &okFiles, count, size)
			if err != nil {
				break
			}
			if key != nil {
				keysToReap = append(keysToReap, *key)
			}
		}
	}

	return keysToReap
}

func (j *janitor) removeFirst(c *cache, items *[]janitorKV, count int, size int64) (*string, int, int64, error) {
	var f janitorKV

	f, *items = (*items)[0], (*items)[1:]

	fileSize, err := c.fs.Size(f.Value.Name())
	if err != nil {
		return nil, count, size, err
	}

	count--
	size = size - fileSize

	return &f.Key, count, size, nil
}
