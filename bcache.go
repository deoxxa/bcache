package bcache // import "fknsrs.biz/p/bcache"

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type Option func(c *Cache)
type Worker func(key string, userdata interface{}) ([]byte, error)
type Strategy func(t time.Time, a, b meta) bool

type Cache struct {
	worker     Worker
	path       string
	keepErrors bool
	lowMark    int
	highMark   int
	maxAge     time.Duration
	strategy   Strategy

	dbLock  sync.RWMutex
	db      *bolt.DB
	dbError error
}

func New(options ...Option) *Cache {
	var c Cache

	for _, fn := range options {
		fn(&c)
	}

	return &c
}

func (c *Cache) ForceInit() error {
	return c.ensureOpen()
}

func (c *Cache) Close() error {
	if c.db == nil {
		return nil
	}

	return c.db.Close()
}

func SetWorker(worker Worker) Option {
	return func(c *Cache) {
		c.worker = worker
	}
}

func SetPath(path string) Option {
	return func(c *Cache) {
		c.path = path
	}
}

func SetKeepErrors(keepErrors bool) Option {
	return func(c *Cache) {
		c.keepErrors = keepErrors
	}
}

func SetHighMark(highMark int) Option {
	return func(c *Cache) {
		c.highMark = highMark
	}
}

func SetLowMark(lowMark int) Option {
	return func(c *Cache) {
		c.lowMark = lowMark
	}
}

func SetMaxAge(age time.Duration) Option {
	return func(c *Cache) {
		c.maxAge = age
	}
}

func SetStrategy(strategy Strategy) Option {
	return func(c *Cache) {
		c.strategy = strategy
	}
}

func (c *Cache) ensureOpen() error {
	c.dbLock.RLock()
	if c.db != nil || c.dbError != nil {
		c.dbLock.RUnlock()
		return c.dbError
	}
	c.dbLock.RUnlock()

	c.dbLock.Lock()
	defer c.dbLock.Unlock()

	if c.db != nil || c.dbError != nil {
		return c.dbError
	}

	if db, err := bolt.Open(c.path, 0644, nil); err != nil {
		c.dbError = err
	} else {
		if err := db.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucketIfNotExists([]byte("meta")); err != nil {
				return err
			}

			if _, err := tx.CreateBucketIfNotExists([]byte("data")); err != nil {
				return err
			}

			if _, err := tx.CreateBucketIfNotExists([]byte("errs")); err != nil {
				return err
			}

			return nil
		}); err != nil {
			c.dbError = err
		} else {
			c.db = db
		}
	}

	return c.dbError
}

type meta struct {
	hash        [20]byte
	createdAt   uint64
	accessedAt  uint64
	accessCount uint64
}

func (m *meta) decode(b []byte) error {
	if len(b) != 24 {
		return errors.New("invalid data length")
	}

	m.createdAt = binary.BigEndian.Uint64(b[0:8])
	m.accessedAt = binary.BigEndian.Uint64(b[8:16])
	m.accessCount = binary.BigEndian.Uint64(b[16:24])

	return nil
}

func (m *meta) encode() []byte {
	var b [24]byte
	binary.BigEndian.PutUint64(b[0:8], m.createdAt)
	binary.BigEndian.PutUint64(b[8:16], m.accessedAt)
	binary.BigEndian.PutUint64(b[16:24], m.accessCount)
	return b[:]
}

func StrategyFIFO() Strategy {
	return func(_ time.Time, a, b meta) bool {
		return a.createdAt < b.createdAt
	}
}

func StrategyLRU() Strategy {
	return func(_ time.Time, a, b meta) bool {
		return a.accessedAt < b.accessedAt
	}
}

func StrategyLFU() Strategy {
	return func(_ time.Time, a, b meta) bool {
		return a.accessCount < b.accessCount
	}
}

func (c *Cache) Get(key string, userdata interface{}) ([]byte, bool, error) {
	return c.GetAt(key, time.Now(), userdata)
}

func (c *Cache) GetAt(key string, t time.Time, userdata interface{}) ([]byte, bool, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, false, err
	}

	h := sha1.New()
	h.Write([]byte(key))
	k := h.Sum(nil)

	var rres []byte
	var isNew bool
	var rerr error

	if err := c.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte("meta"))
		db := tx.Bucket([]byte("data"))
		eb := tx.Bucket([]byte("errs"))

		m := &meta{}

		copy(m.hash[:], k[0:20])

		if d := mb.Get(k); len(d) != 0 {
			if err := m.decode(d); err != nil {
				return err
			}

			if c.maxAge != 0 && time.Duration(t.UnixNano()-int64(m.createdAt)) > c.maxAge {
				if err := mb.Delete(k); err != nil {
					return err
				}
				if err := db.Delete(k); err != nil {
					return err
				}
				if err := eb.Delete(k); err != nil {
					return err
				}

				m.createdAt = uint64(t.UnixNano())
				m.accessedAt = uint64(t.UnixNano())
				m.accessCount = 0
				isNew = true
			}
		} else {
			m.createdAt = uint64(t.UnixNano())
			m.accessedAt = uint64(t.UnixNano())
			m.accessCount = 0
			isNew = true
		}

		if isNew {
			d, err := c.worker(key, userdata)
			if err != nil {
				if !c.keepErrors {
					return err
				}

				if err := eb.Put(k, []byte(err.Error())); err != nil {
					return err
				}

				rerr = errors.New(err.Error())
			}

			if err := db.Put(k, d); err != nil {
				return err
			}

			rres = d
		} else {
			if d := db.Get(k); len(d) > 0 {
				rres = d
			}

			if c.keepErrors {
				if e := eb.Get(k); len(e) > 0 {
					rerr = errors.New(string(e))
				}
			}
		}

		m.accessedAt = uint64(t.UnixNano())
		m.accessCount++
		if err := mb.Put(k, m.encode()); err != nil {
			return err
		}

		if c.highMark != 0 && mb.Stats().KeyN >= c.highMark {
			if err := c.cleanup(tx, t); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, false, err
	}

	return rres, isNew, rerr
}

type metaContext struct {
	t time.Time
	a []meta
	f Strategy
}

func (m *metaContext) add(e meta) { m.a = append(m.a, e) }

func (m *metaContext) Len() int      { return len(m.a) }
func (m *metaContext) Swap(a, b int) { m.a[a], m.a[b] = m.a[b], m.a[a] }

func (m *metaContext) Less(a, b int) bool {
	f := m.f
	if f == nil {
		f = StrategyLRU()
	}

	return f(m.t, m.a[a], m.a[b])
}

func (c *Cache) cleanup(tx *bolt.Tx, t time.Time) error {
	mb := tx.Bucket([]byte("meta"))
	db := tx.Bucket([]byte("data"))
	eb := tx.Bucket([]byte("errs"))

	a := metaContext{t: t, f: c.strategy}

	mb.ForEach(func(k []byte, v []byte) error {
		if len(k) != 20 {
			return errors.New("invalid key length during iteration")
		}

		var m meta
		copy(m.hash[:], k[0:20])
		if err := m.decode(v); err != nil {
			return err
		}

		a.add(m)

		return nil
	})

	// first evict things that have expired
	if c.maxAge != 0 {
		for i := 0; i < len(a.a); i++ {
			if time.Duration(t.UnixNano()-int64(a.a[i].createdAt)) > c.maxAge {
				if err := mb.Delete(a.a[i].hash[:]); err != nil {
					return err
				}
				if err := db.Delete(a.a[i].hash[:]); err != nil {
					return err
				}
				if err := eb.Delete(a.a[i].hash[:]); err != nil {
					return err
				}

				a.a = append(a.a[:i+1], a.a[i+1:]...)
			}
		}
	}

	sort.Sort(&a)
	sort.Reverse(&a)

	toRemove := len(a.a) - c.lowMark
	for i := 0; i < toRemove; i++ {
		if err := mb.Delete(a.a[i].hash[:]); err != nil {
			return err
		}
		if err := db.Delete(a.a[i].hash[:]); err != nil {
			return err
		}
		if err := eb.Delete(a.a[i].hash[:]); err != nil {
			return err
		}
	}

	return nil
}
