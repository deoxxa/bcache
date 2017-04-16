package bcache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var counter int32

func tempfile(prefix string) string {
	return filepath.Join(os.TempDir(), prefix+fmt.Sprintf("%d_%d", time.Now().UnixNano(), atomic.AddInt32(&counter, 1)))
}

func TestNew(t *testing.T) {
	c := New()
	assert.NotNil(t, c)
}

func TestGet(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New(SetPath(path), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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

	c := New(SetPath(path), SetKeepErrors(true), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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

	c := New(SetPath(path), SetMaxAge(time.Minute), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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

	c := New(SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyFIFO()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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
		{"b", true},
		{"a", false},
		{"c", true},
		{"a", false},
		{"d", true},
		{"a", false},
		{"e", true},
		{"a", false},
		{"f", true},
		{"a", true},
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

func TestEvictionLRU(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New(SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyLRU()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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
		{"b", true},
		{"c", true},
		{"d", true},
		{"e", true},
		{"f", true},
		{"a", true},
		{"f", false},
		{"b", true},
		{"c", true},
		{"d", true},
		{"f", false},
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

func TestEvictionLFU(t *testing.T) {
	path := tempfile("test.db")
	defer os.Remove(path)

	count := 0

	c := New(SetPath(path), SetLowMark(3), SetHighMark(5), SetStrategy(StrategyLFU()), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
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

func BenchmarkGet(b *testing.B) {
	m := map[string]Strategy{
		"FIFO": StrategyFIFO(),
		"LRU":  StrategyLRU(),
		"LFU":  StrategyLFU(),
	}

	for _, name := range []string{"FIFO", "LRU", "LFU"} {
		for _, keyCount := range []int{10, 100, 1000, 10000, 100000, 1000000} {
			b.Run(fmt.Sprintf("strategy=%s;keys=%d", name, keyCount), func(b *testing.B) {
				path := tempfile("test.db")
				defer os.Remove(path)

				c := New(SetPath(path), SetLowMark(keyCount/3*2), SetHighMark(keyCount/5*4), SetStrategy(m[name]), SetWorker(func(key string, userdata interface{}) ([]byte, error) {
					return []byte(key), nil
				}))
				defer c.Close()

				for i := 0; i < b.N; i++ {
					if _, _, err := c.Get(makeKey(i%keyCount), nil); err != nil {
						b.FailNow()
					}
				}
			})
		}
	}
}
