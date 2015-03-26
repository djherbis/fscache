package fscache

import "sync"

type closer chan struct{}

func (a closer) Close() error {
	close(a)
	return nil
}

func (a closer) IsOpen() bool {
	select {
	case <-a:
		return false
	default:
		return true
	}
}

type noLock struct{}

func (l noLock) Lock()   {}
func (l noLock) Unlock() {}

type broadcaster struct {
	sync.RWMutex
	closer
	*sync.Cond
}

func newBroadcaster() *broadcaster {
	return &broadcaster{
		Cond:   sync.NewCond(noLock{}),
		closer: make(closer),
	}
}

func (b *broadcaster) Wait() {
	if b.closer.IsOpen() {
		b.Cond.Wait()
	}
}

func (b *broadcaster) Close() error {
	b.closer.Close()
	b.Cond.Broadcast()
	return nil
}
