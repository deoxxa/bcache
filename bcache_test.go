package bcache

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
)

var counter int32

func tempfile(prefix string) string {
	return filepath.Join(os.TempDir(), prefix+fmt.Sprintf("%d_%d", time.Now().UnixNano(), atomic.AddInt32(&counter, 1)))
}

func TestNew(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	c := New("test", SetPath(path))
	assert.NotNil(t, c)
	defer c.Close()

	assert.NoError(t, c.ForceInit())
}

func TestSetDB(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	db, err := bolt.Open(path, 0644, nil)
	assert.NoError(t, err)
	defer db.Close()

	c := New("test", SetDB(db))
	assert.NotNil(t, c)
	assert.NoError(t, c.ForceInit())
	assert.NoError(t, c.Close())

	assert.NoError(t, db.Sync())
}

func TestGet(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		return []byte("hello, " + key), nil
	}))

	assert.NotNil(t, c)

	{
		value, isNew, err := c.Get("a", nil)
		assert.Equal(t, []byte("hello, a"), value)
		assert.True(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.Get("a", nil)
		assert.Equal(t, []byte("hello, a"), value)
		assert.False(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.Get("b", nil)
		assert.Equal(t, []byte("hello, b"), value)
		assert.True(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.Get("b", nil)
		assert.Equal(t, []byte("hello, b"), value)
		assert.False(t, isNew)
		assert.NoError(t, err)
	}

	assert.Equal(t, 2, count)
}

func TestKeepErrors(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetKeepErrors(true), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		if key == "a" {
			return nil, fmt.Errorf("err")
		}

		return []byte(key), nil
	}))
	assert.NotNil(t, c)
	defer c.Close()

	{
		value, isNew, err := c.Get("a", nil)
		assert.Nil(t, value)
		assert.True(t, isNew)
		assert.Error(t, err)
	}

	{
		value, isNew, err := c.Get("a", nil)
		assert.Nil(t, value)
		assert.False(t, isNew)
		assert.Error(t, err)
	}

	{
		value, isNew, err := c.Get("b", nil)
		assert.Equal(t, []byte("b"), value)
		assert.True(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.Get("b", nil)
		assert.Equal(t, []byte("b"), value)
		assert.False(t, isNew)
		assert.NoError(t, err)
	}

	assert.Equal(t, 2, count)
}

func TestMaxAge(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetMaxAge(time.Minute), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		return []byte(key), nil
	}))
	assert.NotNil(t, c)
	defer c.Close()

	t1 := time.Now()
	t2 := time.Now().Add(time.Second * 30)
	t3 := time.Now().Add(time.Minute * 5)

	{
		value, isNew, err := c.GetAt("a", t1, nil)
		assert.Equal(t, []byte("a"), value)
		assert.True(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.GetAt("a", t2, nil)
		assert.Equal(t, []byte("a"), value)
		assert.False(t, isNew)
		assert.NoError(t, err)
	}

	{
		value, isNew, err := c.GetAt("a", t3, nil)
		assert.Equal(t, []byte("a"), value)
		assert.True(t, isNew)
		assert.NoError(t, err)
	}

	assert.Equal(t, 2, count)
}

func TestEvictionFIFO(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyFIFO()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		return []byte(key), nil
	}))
	assert.NotNil(t, c)
	defer c.Close()

	type tc struct {
		k string
		n bool
	}

	for i, e := range []tc{
		{"1", true},
		{"2", true},
		{"1", false},
		{"3", true},
		{"1", false},
		{"4", true},
		{"1", false},
		{"5", true},
		{"1", false},
		{"6", true},
		{"1", true},
	} {
		value, isNew, err := c.Get(e.k, nil)
		assert.Equal(t, []byte(e.k), value)
		assert.NoError(t, err)
		if e.n {
			assert.True(t, isNew, "[%d] %q %v", i, e.k, e.n)
		} else {
			assert.False(t, isNew, "[%d] %q %v", i, e.k, e.n)
		}
	}
}

func TestEvictionLRU(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyLRU()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		return []byte(key), nil
	}))
	assert.NotNil(t, c)
	defer c.Close()

	type tc struct {
		k string
		n bool
	}

	for i, e := range []tc{
		{"1", true},
		{"2", true},
		{"3", true},
		{"4", true},
		{"5", true},
		{"6", true},
		{"1", true},
		{"6", false},
		{"2", true},
		{"3", true},
		{"4", true},
	} {
		value, isNew, err := c.Get(e.k, nil)
		assert.Equal(t, []byte(e.k), value)
		assert.NoError(t, err)
		if e.n {
			assert.True(t, isNew, "[%d] %q %v", i, e.k, e.n)
		} else {
			assert.False(t, isNew, "[%d] %q %v", i, e.k, e.n)
		}
	}
}

func TestEvictionLFU(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New("test", SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyLFU()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
		count++
		return []byte(key), nil
	}))
	assert.NotNil(t, c)
	defer c.Close()

	type tc struct {
		k string
		n bool
	}

	for _, e := range []tc{
		{"a", true},
		{"a", false},
		{"a", false},
		{"a", false},
		{"a", false},
		{"b", true},
		{"c", true},
		{"d", true},
		{"e", true},
		{"f", true},
		{"a", false},
	} {
		value, isNew, err := c.Get(e.k, nil)
		assert.Equal(t, []byte(e.k), value)
		assert.NoError(t, err)
		if e.n {
			assert.True(t, isNew)
		} else {
			assert.False(t, isNew)
		}
	}
}

func makeKey(i int) string {
	return fmt.Sprintf("%08d", i)
}

func BenchmarkGetSerial(b *testing.B) {
	m := map[string]Strategy{
		"FIFO": StrategyFIFO(),
		"LRU":  StrategyLRU(),
		"LFU":  StrategyLFU(),
	}

	rng := rand.NewSource(1)

	for _, name := range []string{"FIFO", "LRU", "LFU"} {
		for _, keyCount := range []int{10, 100, 1000, 10000} {
			b.Run(fmt.Sprintf("strategy=%s;keys=%d", name, keyCount), func(b *testing.B) {
				path := tempfile("test.db")
				defer os.Remove(path)

				c := New("test", SetPath(path), SetLowMark((keyCount/3)*2), SetHighMark((keyCount/5)*4), SetStrategy(m[name]), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
					time.Sleep(time.Millisecond)
					return []byte(key), nil
				}))
				defer c.Close()

				for i := 0; i < b.N; i++ {
					if _, _, err := c.Get(makeKey(int(rng.Int63())%keyCount), nil); err != nil {
						b.FailNow()
					}
				}
			})
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	m := map[string]Strategy{
		"FIFO": StrategyFIFO(),
		"LRU":  StrategyLRU(),
		"LFU":  StrategyLFU(),
	}

	rng := rand.NewSource(1)

	b.SetParallelism(100)

	for _, name := range []string{"FIFO", "LRU", "LFU"} {
		for _, keyCount := range []int{10, 100, 1000, 10000} {
			b.Run(fmt.Sprintf("strategy=%s;keys=%d", name, keyCount), func(b *testing.B) {
				path := tempfile("test.db")
				defer os.Remove(path)

				c := New("test", SetPath(path), SetLowMark((keyCount/3)*2), SetHighMark((keyCount/5)*4), SetStrategy(m[name]), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
					time.Sleep(time.Millisecond)
					return []byte(key), nil
				}))
				defer c.Close()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						c.Get(makeKey(int(rng.Int63())%keyCount), nil)
					}
				})
			})
		}
	}
}
