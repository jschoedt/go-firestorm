package cache

import (
	"context"
	"github.com/jschoedt/go-firestorm"
	"github.com/patrickmn/go-cache"
	"time"
)

type InMemoryCache struct {
	c *cache.Cache
}

func NewMemoryCache(defaultExpiration, cleanupInterval time.Duration) *InMemoryCache {
	return &InMemoryCache{
		c: cache.New(defaultExpiration, cleanupInterval),
	}
}

func (m *InMemoryCache) Get(ctx context.Context, key string) (firestorm.EntityMap, error) {
	if elm, ok := m.c.Get(key); ok {
		if m, ok := elm.(firestorm.EntityMap); ok {
			return m.Copy(), nil
		}
	}
	return nil, firestorm.ErrCacheMiss
}

func (m *InMemoryCache) GetMulti(ctx context.Context, keys []string) (map[string]firestorm.EntityMap, error) {
	result := make(map[string]firestorm.EntityMap, len(keys))
	for _, k := range keys {
		if m, err := m.Get(ctx, k); err == nil {
			result[k] = m
		}
	}
	return result, nil

}

func (m *InMemoryCache) Set(ctx context.Context, key string, item firestorm.EntityMap) error {
	m.c.Set(key, item, cache.DefaultExpiration)
	return nil
}

func (m *InMemoryCache) SetMulti(ctx context.Context, items map[string]firestorm.EntityMap) error {
	for i, elm := range items {
		m.Set(ctx, i, elm)
	}
	return nil
}

func (m *InMemoryCache) Delete(ctx context.Context, key string) error {
	m.c.Delete(key)
	return nil
}

func (m *InMemoryCache) DeleteMulti(ctx context.Context, keys []string) error {
	for _, key := range keys {
		m.Delete(ctx, key)
	}
	return nil
}
