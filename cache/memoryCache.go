package cache

import (
	"context"
	"github.com/jschoedt/go-firestorm"
	"github.com/patrickmn/go-cache"
	"reflect"
	"time"
)

type MemoryCache struct {
	c *cache.Cache
}

func NewMemoryCache(defaultExpiration, cleanupInterval time.Duration) *MemoryCache {
	return &MemoryCache{
		c: cache.New(defaultExpiration, cleanupInterval),
	}
}

func (m *MemoryCache) Get(c context.Context, key string, v interface{}) error {
	if elm, ok := m.c.Get(key); ok {
		val := reflect.Indirect(reflect.ValueOf(v))
		val.Set(reflect.Indirect(reflect.ValueOf(elm)))
	}
	return firestorm.ErrCacheMiss
}

func (m *MemoryCache) GetMulti(c context.Context, vs map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(vs))
	for k := range vs {
		if e, ok := m.c.Get(k); ok {
			result[k] = e
		}
	}
	return result, nil

}

func (m *MemoryCache) Set(c context.Context, key string, item interface{}) error {
	m.c.Set(key, item, cache.DefaultExpiration)
	return nil
}

func (m *MemoryCache) SetMulti(c context.Context, items map[string]interface{}) error {
	for i, elm := range items {
		m.Set(c, i, elm)
	}
	return nil
}

func (m *MemoryCache) Delete(c context.Context, key string) error {
	m.c.Delete(key)
	return nil
}

func (m *MemoryCache) DeleteMulti(c context.Context, keys []string) error {
	for _, key := range keys {
		m.Delete(c, key)
	}
	return nil
}
