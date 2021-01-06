package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
)

var (
	// SessionCacheKey is the key for the session map in the context
	SessionCacheKey = contextKey("sessionCache")
	// ErrCacheMiss returned on a cache miss
	ErrCacheMiss = errors.New("not found in cache")
	logOnce      sync.Once
)

const cacheElement = "_cacheElement"
const cacheSlice = "_cacheSlice"

// CacheHandler should be used on the mux chain to support session cache.
// So getting the same entity several times will only generate on DB hit
func CacheHandler(next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), SessionCacheKey, make(map[string]EntityMap))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Cache can be used to implement custom caching
type Cache interface {
	Get(ctx context.Context, key string) (EntityMap, error)
	GetMulti(ctx context.Context, keys []string) (map[string]EntityMap, error)
	Set(ctx context.Context, key string, item EntityMap) error
	SetMulti(ctx context.Context, items map[string]EntityMap) error
	Delete(ctx context.Context, key string) error
	DeleteMulti(ctx context.Context, keys []string) error
}

type cacheWrapper struct {
	client *firestore.Client
	first  Cache
	second Cache
}

func newCacheWrapper(client *firestore.Client, first Cache, second Cache) *cacheWrapper {
	cw := &cacheWrapper{}
	cw.client = client
	cw.first = first
	cw.second = second
	return cw
}

func (c *cacheWrapper) convertToCacheRef(m EntityMap, ref *firestore.DocumentRef) cacheRef {
	c.makeUnCachable(m)
	return newCacheRef(m, ref)
}

func (c *cacheWrapper) Get(ctx context.Context, ref *firestore.DocumentRef) (cacheRef, error) {
	m, err := c.first.Get(ctx, ref.Path)
	if err == ErrCacheMiss && c.second != nil {
		m, err = c.second.Get(ctx, ref.Path)
	}

	//log.Printf("Get: ID: %v - %+v\n", ref.Path, m)
	return c.convertToCacheRef(m, ref), err
}

func (c *cacheWrapper) GetMulti(ctx context.Context, refs []*firestore.DocumentRef) (map[*firestore.DocumentRef]cacheRef, error) {
	keys := make([]string, 0, len(refs))
	keyToRef := make(map[string]*firestore.DocumentRef, len(refs))
	result := make(map[*firestore.DocumentRef]cacheRef, len(refs))

	for _, ref := range refs {
		keys = append(keys, ref.Path)
		keyToRef[ref.Path] = ref
	}

	first, err := c.first.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	if c.second == nil {
		for key, val := range first {
			ref := keyToRef[key]
			result[ref] = c.convertToCacheRef(val, ref) // update result with first level
		}
		return result, nil
	}

	// deep && c.second != nil - find remaining
	var remaining []string
	for _, key := range keys {
		if val, ok := first[key]; ok {
			ref := keyToRef[key]
			result[ref] = c.convertToCacheRef(val, ref) // update result with first level
		} else {
			remaining = append(remaining, key)
		}
	}
	// get the diff list
	if second, err := c.second.GetMulti(ctx, remaining); err != nil {
		return nil, err
	} else {
		for key, elm := range second {
			ref := keyToRef[key]
			result[ref] = c.convertToCacheRef(elm, ref) // update result with first level
		}
	}

	return result, nil
}

func (c *cacheWrapper) Set(ctx context.Context, key string, item map[string]interface{}) error {
	//log.Printf("Set: ID: %v - %+v\n", key, item)
	c.makeCachable(item)
	if err := c.first.Set(ctx, key, item); err != nil {
		return err
	}
	if c.second != nil {
		return c.second.Set(ctx, key, item)
	}
	return nil
}

func (c *cacheWrapper) SetMulti(ctx context.Context, items map[string]EntityMap) error {
	if len(items) == 0 {
		return nil
	}
	//log.Printf("Set multi: %+v\n", items)
	cache := make(map[string]EntityMap, len(items))
	for k, v := range items {
		c.makeCachable(v)
		cache[k] = v
	}
	if err := c.first.SetMulti(ctx, cache); err != nil {
		return err
	}
	if c.second != nil {
		return c.second.SetMulti(ctx, cache)
	}
	return nil
}

func (c *cacheWrapper) Delete(ctx context.Context, key string) error {
	if err := c.first.Delete(ctx, key); err != nil {
		return err
	}
	if c.second != nil {
		return c.second.Delete(ctx, key)
	}
	return nil
}

func (c *cacheWrapper) DeleteMulti(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.first.DeleteMulti(ctx, keys); err != nil {
		return err
	}
	if c.second != nil {
		return c.second.DeleteMulti(ctx, keys)
	}
	return nil
}

func (c *cacheWrapper) makeCachable(m map[string]interface{}) {
	const sep = "/documents/" // for some reason Firestore cant use the full path so cut it
	for k, v := range m {
		switch val := v.(type) {
		case *firestore.DocumentRef:
			m[k+cacheElement] = strings.Split(val.Path, sep)[1]
			delete(m, k)
		default:
			valOf := reflect.ValueOf(v)
			switch valOf.Kind() {
			case reflect.Slice:
				if valOf.Len() > 0 {
					first := valOf.Index(0)
					if first.Kind() == reflect.Interface && first.Elem().Type() == refType {
						refs := make([]string, valOf.Len())
						for i := 0; i < valOf.Len(); i++ {
							fromEmlPtr := valOf.Index(i)
							refs[i] = strings.Split(fromEmlPtr.Interface().(*firestore.DocumentRef).Path, sep)[1]
						}
						m[k+cacheSlice] = refs
						delete(m, k)
					}
				}
			}
		}
	}
}

func (c *cacheWrapper) makeUnCachable(m map[string]interface{}) {
	for k, v := range m {
		if strings.HasSuffix(k, cacheElement) {
			m[strings.Replace(k, cacheElement, "", -1)] = c.client.Doc(v.(string))
			delete(m, k)
		} else if strings.HasSuffix(k, cacheSlice) {
			// interface type to be consistent with firestorm arrays
			res := make([]interface{}, len(v.([]string)))
			for i, v := range v.([]string) {
				res[i] = c.client.Doc(v)
			}
			m[strings.Replace(k, cacheSlice, "", -1)] = res
			delete(m, k)
		}
	}
}

type defaultCache struct {
	sync.RWMutex
}

func newDefaultCache() *defaultCache {
	return &defaultCache{}
}

func (c *defaultCache) Get(ctx context.Context, key string) (EntityMap, error) {
	c.RLock()
	defer c.RUnlock()
	e, ok := getSessionCache(ctx)[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	m := e.Copy()
	return m, nil
}

func (c *defaultCache) GetMulti(ctx context.Context, keys []string) (map[string]EntityMap, error) {
	c.RLock()
	defer c.RUnlock()
	result := make(map[string]EntityMap, len(keys))
	for _, k := range keys {
		if e, ok := getSessionCache(ctx)[k]; ok {
			result[k] = e.Copy()
		}
	}
	return result, nil
}

func (c *defaultCache) Set(ctx context.Context, key string, item EntityMap) error {
	c.Lock()
	defer c.Unlock()
	getSessionCache(ctx)[key] = item
	return nil
}

func (c *defaultCache) SetMulti(ctx context.Context, items map[string]EntityMap) error {
	c.Lock()
	defer c.Unlock()
	for k, v := range items {
		getSessionCache(ctx)[k] = v
	}
	return nil
}

func (c *defaultCache) Delete(ctx context.Context, key string) error {
	return c.Set(ctx, key, nil)
}

func (c *defaultCache) DeleteMulti(ctx context.Context, keys []string) error {
	for _, key := range keys {
		c.Delete(ctx, key)
	}
	return nil
}

func (c *defaultCache) getSetRec(ctx context.Context) map[string]EntityMap {
	result := make(map[string]EntityMap)
	for key, elm := range getSessionCache(ctx) {
		if elm != nil {
			result[key] = elm
		}
	}
	return result
}

func (c *defaultCache) getDeleteRec(ctx context.Context) []string {
	var result []string
	for key, elm := range getSessionCache(ctx) {
		if elm == nil {
			result = append(result, key)
		}
	}
	return result
}

func getSessionCache(ctx context.Context) map[string]EntityMap {
	if c, ok := ctx.Value(SessionCacheKey).(map[string]EntityMap); ok {
		return c
	}
	logOnce.Do(func() {
		log.Println("Warning. Consider adding the CacheHandler middleware for the session cache to work")
	})
	return make(map[string]EntityMap)
}
