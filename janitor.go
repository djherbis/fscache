package fscache

import (
	"sort"
	"time"
)

type janitorKV struct {
	Key   string
	Value fileStream
}

type CacheStats struct {
	// public fields / methods to provide info about the Cache state
	// this can expose the extra info needed by Janitor to decide what to reap
	cache *cache
}

type StatsBasedReaper interface {
	ReapUsingStats(ctx CacheStats) (keysToReap []string)
}

// Janitor is used to control when there are too many streams
// or the size of the streams is too big.
// It is called once right after loading, and then it is run
// again after every Next() Period of time.
// Janitor runs every "Period" and scrubs older files
// when the total file size is over MaxTotalFileSize or
// total item count is over MaxItems.
// If MaxItems or MaxTotalFileSize are 0, they won't be checked
type Janitor struct {
	Period           time.Duration
	MaxItems         int
	MaxTotalFileSize int64
}

func (j *Janitor) Next() time.Duration {
	return j.Period
}

func (j *Janitor) Reap(_ string, _, _ time.Time) bool {
	// not implemented: should not get here
	return false
}

func (j *Janitor) ReapUsingStats(ctx CacheStats) (keysToReap []string) {
	if len(ctx.cache.files) == 0 {
		return
	}

	var count int
	var size int64
	var okFiles []janitorKV

	for k, f := range ctx.cache.files {
		if f.inUse() {
			continue
		}

		fileSize, err := ctx.cache.fs.Size(f.Name())
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
		iLastRead, _, err := ctx.cache.fs.AccessTimes(okFiles[i].Value.Name())
		if err != nil {
			return false
		}

		jLastRead, _, err := ctx.cache.fs.AccessTimes(okFiles[l].Value.Name())
		if err != nil {
			return false
		}

		return iLastRead.Before(jLastRead)
	})

	if j.MaxItems > 0 {
		for count > j.MaxItems {
			var key *string
			var err error
			key, count, size, err = j.removeFirst(ctx.cache, &okFiles, count, size)
			if err != nil {
				break
			}
			if key != nil {
				keysToReap = append(keysToReap, *key)
			}
		}
	}

	if j.MaxTotalFileSize > 0 {
		for size > j.MaxTotalFileSize {
			var key *string
			var err error
			key, count, size, err = j.removeFirst(ctx.cache, &okFiles, count, size)
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

func (j *Janitor) removeFirst(c *cache, items *[]janitorKV, count int, size int64) (*string, int, int64, error) {
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
