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
		ctx := context.WithValue(r.Context(), SessionCacheKey, make(map[string]interface{}))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Cache can be used to implement custom caching
type Cache interface {
	Get(c context.Context, key string, v interface{}) error
	GetMulti(c context.Context, vs map[string]interface{}) (map[string]interface{}, error)
	Set(c context.Context, key string, item interface{}) error
	SetMulti(c context.Context, items map[string]interface{}) error
	Delete(c context.Context, key string) error
	DeleteMulti(c context.Context, keys []string) error
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

func (c *cacheWrapper) convertToCacheRef(m map[string]interface{}, ref *firestore.DocumentRef) cacheRef {
	c.makeUnCachable(m)
	return newCacheRef(m, ref)
}

func (c *cacheWrapper) Get(ctx context.Context, ref *firestore.DocumentRef, deep bool) (cacheRef, error) {
	var m map[string]interface{}
	err := c.first.Get(ctx, ref.Path, &m)
	if err == ErrCacheMiss && c.second != nil {
		err = c.second.Get(ctx, ref.Path, &m)
	}

	//log.Printf("Get: ID: %v - %+v\n", ref.Path, m)
	return c.convertToCacheRef(m, ref), err
}

func (c *cacheWrapper) GetMulti(ctx context.Context, refs []*firestore.DocumentRef) (map[*firestore.DocumentRef]cacheRef, error) {
	keyToRef := make(map[string]*firestore.DocumentRef, len(refs))
	tmpResult := make(map[string]interface{}, len(refs))
	result := make(map[*firestore.DocumentRef]cacheRef, len(tmpResult))
	for _, ref := range refs {
		keyToRef[ref.Path] = ref
		tmpResult[ref.Path] = make(map[string]interface{})
	}

	if first, err := c.first.GetMulti(ctx, tmpResult); err != nil {
		return nil, err
	} else if c.second == nil {
		for key, val := range first {
			ref := keyToRef[key]
			result[ref] = c.convertToCacheRef(val.(map[string]interface{}), ref) // update result with first level
		}
	} else { // deep && c.second != nil
		for key := range tmpResult {
			if val, ok := first[key]; ok {
				ref := keyToRef[key]
				result[ref] = c.convertToCacheRef(val.(map[string]interface{}), ref) // update result with first level
				delete(tmpResult, key)                                               // remove it since we found it
			}
		}
		// get the diff list
		if multi, err := c.second.GetMulti(ctx, tmpResult); err != nil {
			return nil, err
		} else {
			for key, elm := range multi {
				ref := keyToRef[key]
				result[ref] = c.convertToCacheRef(elm.(map[string]interface{}), ref) // update result with first level
			}
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

func (c *cacheWrapper) SetMulti(ctx context.Context, items map[string]map[string]interface{}) error {
	if len(items) == 0 {
		return nil
	}
	//log.Printf("Set multi: %+v\n", items)
	cache := make(map[string]interface{}, len(items))
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

func (c *defaultCache) Get(ctx context.Context, key string, v interface{}) error {
	c.RLock()
	defer c.RUnlock()
	e, ok := getSessionCache(ctx)[key]
	if !ok {
		return ErrCacheMiss
	}
	// set the value using reflection
	val := reflect.Indirect(reflect.ValueOf(v))
	val.Set(reflect.Indirect(reflect.ValueOf(e)))
	return nil
}

func (c *defaultCache) GetMulti(ctx context.Context, vs map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(vs))
	c.RLock()
	defer c.RUnlock()
	for k := range vs {
		if e, ok := getSessionCache(ctx)[k]; ok {
			result[k] = e
		}
	}
	return result, nil
}

func (c *defaultCache) Set(ctx context.Context, key string, item interface{}) error {
	c.Lock()
	defer c.Unlock()
	getSessionCache(ctx)[key] = item
	return nil
}

func (c *defaultCache) SetMulti(ctx context.Context, items map[string]interface{}) error {
	c.Lock()
	defer c.Unlock()
	for k, v := range items {
		getSessionCache(ctx)[k] = v
	}
	return nil
}

func (c *defaultCache) Delete(ctx context.Context, key string) error {
	c.Lock()
	defer c.Unlock()
	delete(getSessionCache(ctx), key)
	return nil
}

func (c *defaultCache) DeleteMulti(ctx context.Context, keys []string) error {
	c.Lock()
	defer c.Unlock()
	for _, k := range keys {
		delete(getSessionCache(ctx), k)
	}
	return nil
}

func getSessionCache(ctx context.Context) map[string]interface{} {
	if c, ok := ctx.Value(SessionCacheKey).(map[string]interface{}); ok {
		return c
	}
	logOnce.Do(func() {
		log.Println("Warning. Consider adding the CacheHandler middleware for the session cache to work")
	})
	return make(map[string]interface{})
}

type recordingCache struct {
	updated []string
}

func newRecordingCache() *recordingCache {
	return &recordingCache{}
}

func (r *recordingCache) Get(c context.Context, key string, v interface{}) error {
	return nil
}

func (r recordingCache) GetMulti(c context.Context, vs map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func (r *recordingCache) Set(c context.Context, key string, item interface{}) error {
	r.updated = append(r.updated, key)
	return nil
}

func (r *recordingCache) SetMulti(c context.Context, items map[string]interface{}) error {
	for key := range items {
		r.updated = append(r.updated, key)
	}
	return nil
}

func (r *recordingCache) Delete(c context.Context, key string) error {
	r.updated = append(r.updated, key)
	return nil
}

func (r *recordingCache) DeleteMulti(c context.Context, keys []string) error {
	r.updated = append(r.updated, keys...)
	return nil
}
