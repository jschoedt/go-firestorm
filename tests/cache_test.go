package firestormtests

import (
	"context"
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/jschoedt/go-firestorm"
	"github.com/jschoedt/go-firestorm/cache"
	"testing"
	"time"
)

func TestCacheCRUD(t *testing.T) {
	ctx := createSessionCacheContext()
	memoryCache := cache.NewMemoryCache(5*time.Minute, 10*time.Minute)
	fsc.SetCache(memoryCache)

	car := &Car{}
	car.ID = "MyCar"
	car.Make = "Toyota"

	// Create the entity
	fsc.NewRequest().CreateEntities(ctx, car)()
	assertInCache(ctx, memoryCache, car, t)

	// Update the entity
	car.Make = "Jeep"
	fsc.NewRequest().UpdateEntities(ctx, car)()
	m := assertInCache(ctx, memoryCache, car, t)
	if m["make"] != car.Make {
		t.Errorf("Value should be: %v - but was: %v", car.Make, m["Make"])
	}

	// Delete the entity
	fsc.NewRequest().DeleteEntities(ctx, car)()
	assertNotInCache(ctx, memoryCache, car, t)
}

func TestCacheTransaction(t *testing.T) {
	ctx := createSessionCacheContext()
	memoryCache := cache.NewMemoryCache(1*time.Second, 10*time.Minute)
	fsc.SetCache(memoryCache)

	car := &Car{}
	car.ID = "MyCar"
	car.Make = "Toyota"

	fsc.DoInTransaction(ctx, func(tctx context.Context) error {
		// Create the entity
		fsc.NewRequest().CreateEntities(tctx, car)()
		assertInSessionCache(tctx, car, t)
		assertNotInCache(ctx, memoryCache, car, t)
		return errors.New("rollback")
	})

	assertNotInCache(ctx, memoryCache, car, t)

	fsc.NewRequest().GetEntities(ctx, car)()
	assertInCache(ctx, memoryCache, car, t)

	car.Make = "Jeep"

	fsc.DoInTransaction(ctx, func(tctx context.Context) error {
		// Create the entity
		fsc.NewRequest().UpdateEntities(tctx, car)()
		assertInSessionCache(tctx, car, t)
		assertInCache(ctx, memoryCache, car, t)
		return nil
	})

	assertNotInCache(ctx, memoryCache, car, t)

	// Delete the entity
	fsc.NewRequest().DeleteEntities(ctx, car)()
	assertNotInCache(ctx, memoryCache, car, t)
}

func assertInSessionCache(ctx context.Context, car *Car, t *testing.T) {
	cacheKey := fsc.NewRequest().ToRef(car).Path
	sessionCache := getSessionCache(ctx)

	if sessionCache[cacheKey] == nil {
		t.Errorf("entity not found in session cache : %v", cacheKey)
	}
}

func assertInCache(ctx context.Context, memoryCache *cache.MemoryCache, car *Car, t *testing.T) map[string]interface{} {
	cacheKey := fsc.NewRequest().ToRef(car).Path
	sessionCache := getSessionCache(ctx)

	assertInSessionCache(ctx, car, t)
	m := make(map[string]interface{})
	if err := memoryCache.Get(ctx, cacheKey, &m); err == nil {
		t.Errorf("entity not found in cache : %v", cacheKey)
	}

	if !cmp.Equal(sessionCache[cacheKey], m) {
		t.Errorf("The elements were not the same %v", cmp.Diff(sessionCache[cacheKey], m))
	}
	return m
}

func assertNotInCache(ctx context.Context, memoryCache *cache.MemoryCache, car *Car, t *testing.T) {
	cacheKey := fsc.NewRequest().ToRef(car).Path
	sessionCache := getSessionCache(ctx)

	if sessionCache[cacheKey] != nil {
		t.Errorf("entity should not be in session cache : %v", cacheKey)
	}
	m := make(map[string]interface{})
	if err := memoryCache.Get(ctx, cacheKey, &m); err != firestorm.ErrCacheMiss {
		t.Errorf("entity should not be in cache : %v", cacheKey)
	}
}
