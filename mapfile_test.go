package fscache_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/djherbis/fscache"
)

// TestMapFile demonstrates and tests the proposed FSCache.MapFile mechanism.
// The theory is that if a cache entry already exists as a file on disk,
// and the cache is backed by a filesystem, we can avoid the copy operation
// that occurs when the cache is filled, by instead mapping the file into
// the cache. This is a serious win for large files.
//
// Our example scenario is a Getter interface that gets a reader for a URL
// or filepath.
//
// Two implementations of Getter are provided: StdGetter and MapGetter. Both
// implementations are tested against the local filesystem README.md, and
// the README.md hosted on GitHub via https, using fscache.StandardFS, and
// also fscache.NewMemFs.
//
// Both Getter implementations behave the same for the HTTP case. For the
// file case (which is what we're really interested in), StdGetter uses
// the standard cache-filling mechanism, which is to copy the file content
// bytes from disk into the w returned by FSCache.Get. This is the scenario
// that we're trying to address: the goal is to avoid this unnecessary
// copy operation.
//
// Meanwhile, for the file case, MapGetter uses the new FSCache.MapFile
// mechanism, which avoids the copy operation.
func TestMapFile(t *testing.T) {
	const iterations = 5
	const readmeHTTP = "https://raw.githubusercontent.com/djherbis/fscache/master/README.md"
	readmeFilepath, err := filepath.Abs("README.md")
	require.NoError(t, err)

	newDiskFs := func() fscache.FileSystem {
		dir, err := ioutil.TempDir("", "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		fs, err := fscache.NewFs(dir, os.ModePerm)
		require.NoError(t, err)
		return fs
	}

	testCases := []struct {
		name          string
		getterFactory func(t *testing.T, fs fscache.FileSystem) Getter
		fsFactory     func() fscache.FileSystem
		src           string
	}{
		{"map_diskfs_http", NewMapGetter, newDiskFs, readmeHTTP},
		{"map_diskfs_file", NewMapGetter, newDiskFs, readmeFilepath},
		{"map_memfs_http", NewMapGetter, fscache.NewMemFs, readmeHTTP},
		{"map_memfs_file", NewMapGetter, fscache.NewMemFs, readmeFilepath},
		{"std_diskfs_http", NewStdGetter, newDiskFs, readmeHTTP},
		{"std_diskfs_file", NewStdGetter, newDiskFs, readmeFilepath},
		{"std_memfs_http", NewStdGetter, fscache.NewMemFs, readmeHTTP},
		{"std_memfs_file", NewStdGetter, fscache.NewMemFs, readmeFilepath},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			g := tc.getterFactory(t, tc.fsFactory())
			for i := 0; i < iterations; i++ {
				rc, err := g.Get(tc.src)
				require.NoError(t, err)
				require.NotNil(t, rc)
				b, err := ioutil.ReadAll(rc)
				assert.NoError(t, rc.Close())
				require.NoError(t, err)
				require.Contains(t, string(b), "Streaming File Cache for #golang")
			}

			// Make sure that calling FSCache.Remove doesn't actually
			// delete the file from disk.
			if tc.src == readmeFilepath {
				if mg, ok := g.(*MapGetter); ok {
					err = mg.fc.Remove(tc.src)
					require.NoError(t, err)
					fi, err := os.Stat(tc.src)
					require.NoError(t, err)
					require.Equal(t, filepath.Base(tc.src), fi.Name())
				}
			}
		})
	}
}

// Getter gets a reader for a URL or filepath.
type Getter interface {
	Get(urlOrFilepath string) (io.ReadCloser, error)
}

// NewStdGetter is a factory function for StdGetter.
func NewStdGetter(t *testing.T, fs fscache.FileSystem) Getter {
	g := &StdGetter{logf: t.Logf}
	var err error
	g.fc, err = fscache.NewCache(fs, nil)
	require.NoError(t, err)
	return g
}

var _ Getter = (*StdGetter)(nil)

// StdGetter is a getter that uses the standard cache-filling mechanism.
type StdGetter struct {
	fc   *fscache.FSCache
	logf func(format string, args ...interface{})
}

// Get implements Getter.
func (g *StdGetter) Get(urlOrFilepath string) (io.ReadCloser, error) {
	if strings.HasPrefix(urlOrFilepath, "http://") ||
		strings.HasPrefix(urlOrFilepath, "https://") {

		r, w, err := g.fc.Get(urlOrFilepath)
		if err != nil {
			return nil, err
		}

		if w == nil {
			g.logf("Cache hit: %s", urlOrFilepath)
			return r, nil
		}

		g.logf("Cache miss: %s", urlOrFilepath)

		if err = httpGet(urlOrFilepath, w); err != nil {
			return nil, err
		}

		g.logf("Fetched: %s", urlOrFilepath)
		return r, nil
	}

	// Thus, urlOrFilepath must be a filepath.
	fp := urlOrFilepath
	r, w, err := g.fc.Get(fp)
	if err != nil {
		return nil, err
	}

	if w == nil {
		g.logf("Cache hit: %s", fp)
		return r, nil
	}

	g.logf("Cache miss: %s", fp)

	f, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// We copy the contents of f to w and thus into the cache.
	// But, for our use-case, this is useless work.
	// We're already using a filesystem FSCache, so we're just
	// copying the file from disk to memory and back to disk.
	// Boo!
	var n int64
	if n, err = io.Copy(w, f); err != nil {
		return nil, err
	}

	if err = w.Close(); err != nil {
		return nil, err
	}
	g.logf("EXPENSIVE: Copied %d bytes from %s to cache", n, fp)
	return r, nil
}

// NewMapGetter is a factory function for MapGetter.
func NewMapGetter(t *testing.T, fs fscache.FileSystem) Getter {
	g := &MapGetter{logf: t.Logf}
	var err error
	g.fc, err = fscache.NewCache(fs, nil)
	require.NoError(t, err)
	return g
}

var _ Getter = (*MapGetter)(nil)

// MapGetter is a Getter that uses the new FSCache.MapFile mechanism to
// map existing files into the cache.
type MapGetter struct {
	fc   *fscache.FSCache
	logf func(format string, args ...interface{})
}

// Get implements Getter.
func (g *MapGetter) Get(urlOrFilepath string) (io.ReadCloser, error) {
	if strings.HasPrefix(urlOrFilepath, "http://") ||
		strings.HasPrefix(urlOrFilepath, "https://") {

		r, w, err := g.fc.Get(urlOrFilepath)
		if err != nil {
			return nil, err
		}

		if w == nil {
			g.logf("Cache hit: %s", urlOrFilepath)
			return r, nil
		}

		g.logf("Cache miss: %s", urlOrFilepath)

		if err = httpGet(urlOrFilepath, w); err != nil {
			return nil, err
		}

		g.logf("Fetched: %s", urlOrFilepath)
		return r, nil
	}

	// Thus, urlOrFilepath must be a filepath.
	fp := urlOrFilepath
	if g.fc.Exists(fp) {
		g.logf("Cache hit: %s", fp)
		r, _, err := g.fc.Get(fp)
		return r, err
	}

	g.logf("Cache miss: %s", fp)
	g.logf("Mapping file into cache: %s", fp)

	if err := g.fc.MapFile(fp); err != nil {
		return nil, err
	}

	r, _, err := g.fc.Get(fp)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// httpGet writes the contents at URL u to w (which
// is always closed).
func httpGet(u string, w io.WriteCloser) error {
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if _, err = io.Copy(w, resp.Body); err != nil {
		_ = w.Close()
		return err
	}

	return w.Close()
}
