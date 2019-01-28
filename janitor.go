package fscache

import (
	"sort"
	"time"
)

type janitorKV struct {
	Key   string
	Value Entry
}

// Janitor is used to control when there are too many streams
// or the size of the streams is too big.
// It is called once right after loading, and then it is run
// again after every Next() period of time.
type Janitor interface {
	// Returns the amount of time to wait before the next scheduled Reaping.
	Next() time.Duration

	// Given a CacheAccessor, return keys to reap list.
	Scrub(c CacheAccessor) []string
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

func (j *janitor) Scrub(c CacheAccessor) (keysToReap []string) {
	var count int
	var size int64
	var okFiles []janitorKV

	c.EnumerateEntries(func(key string, e Entry) bool {
		if e.InUse() {
			return true
		}

		fileInfo, err := c.Stat(e.Name())
		if err != nil {
			return true
		}

		count++
		size = size + fileInfo.Size()
		okFiles = append(okFiles, janitorKV{
			Key:   key,
			Value: e,
		})

		return true
	})

	sort.Slice(okFiles, func(i, l int) bool {
		iFileInfo, err := c.Stat(okFiles[i].Value.Name())
		if err != nil {
			return false
		}

		iLastRead, _, err := iFileInfo.AccessTimes()
		if err != nil {
			return false
		}

		jFileInfo, err := c.Stat(okFiles[i].Value.Name())
		if err != nil {
			return false
		}

		jLastRead, _, err := jFileInfo.AccessTimes()
		if err != nil {
			return false
		}

		return iLastRead.Before(jLastRead)
	})

	collectKeysToReapFn := func() bool {
		var key *string
		var err error
		key, count, size, err = j.removeFirst(c, &okFiles, count, size)
		if err != nil {
			return false
		}
		if key != nil {
			keysToReap = append(keysToReap, *key)
		}

		return true
	}

	if j.maxItems > 0 {
		for count > j.maxItems {
			if !collectKeysToReapFn() {
				break
			}
		}
	}

	if j.maxSize > 0 {
		for size > j.maxSize {
			if !collectKeysToReapFn() {
				break
			}
		}
	}

	return keysToReap
}

func (j *janitor) removeFirst(fsStater FileSystemStater, items *[]janitorKV, count int, size int64) (*string, int, int64, error) {
	var f janitorKV

	f, *items = (*items)[0], (*items)[1:]

	fileInfo, err := fsStater.Stat(f.Value.Name())
	if err != nil {
		return nil, count, size, err
	}

	count--
	size = size - fileInfo.Size()

	return &f.Key, count, size, nil
}
