package fscache

import "time"

// Reaper is used to control when streams expire from the cache.
// It is called once right after loading, and then it is run
// again after every Next() period of time.
// Reap() takes the key and the last access time of a stream in the cache.
// Return true to remove the key, false to keep it.
type Reaper interface {
	Next() time.Duration
	Reap(string, time.Time) bool
}

// NewReaper returns a simple reaper which runs every "period"
// and reaps files which are older than "expiry".
func NewReaper(expiry, period time.Duration) Reaper {
	return &reaper{
		expiry: expiry,
		period: period,
	}
}

type reaper struct {
	period time.Duration
	expiry time.Duration
}

func (g *reaper) Next() time.Duration {
	return g.period
}

func (g *reaper) Reap(key string, lastRead time.Time) bool {
	return lastRead.Before(time.Now().Add(-g.expiry))
}
