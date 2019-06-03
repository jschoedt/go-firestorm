package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
)

var (
	ContextKeySCache = contextKey("sessionCache")
	CacheMiss        = errors.New("not found in cache")
)

const cacheElement = "_cacheElement"
const cacheSlice = "_cacheSlice"

type contextKey string

func (c contextKey) String() string {
	return "context key " + string(c)
}

type Cache interface {
	Get(c context.Context, key string, v interface{}) error
	//GetMulti(c context.Context, vs map[string]interface{}) (map[string]interface{}, error)
	Set(c context.Context, key string, item interface{}) error
	SetMulti(c context.Context, items map[string]interface{}) error
	Delete(c context.Context, key string) error
	DeleteMulti(c context.Context, keys []string) error
}

type CacheWrapper struct {
	client *firestore.Client
	first  Cache
	second Cache
}

func NewCacheWrapper(client *firestore.Client, first Cache, second Cache) *CacheWrapper {
	cw := &CacheWrapper{}
	cw.client = client
	cw.first = first
	cw.second = second
	return cw
}

func (c *CacheWrapper) Get(ctx context.Context, ref *firestore.DocumentRef, deep bool) (CacheRef, error) {
	m := make(map[string]interface{})
	err := c.first.Get(ctx, ref.Path, &m)
	if err == CacheMiss && deep && c.second != nil {
		err = c.second.Get(ctx, ref.Path, &m)
	}
	c.makeUnCachable(m)
	//log.Printf("Get: ID: %v - %+v\n", ref.Path, m)
	return NewCacheRef(m, ref), err
}

/*
func (c *CacheWrapper) GetMulti(ctx context.Context, keys []string, deep bool) (map[string]interface{}, error) {
	if e, err := c.first.GetMulti(ctx, keys); err != nil {
		return nil, err
	} else if e != nil {
		return e, nil
	}
	if deep && c.second != nil {
		return c.second.GetMulti(ctx, keys)
	}
	return nil, nil
}

*/

func (c *CacheWrapper) Set(ctx context.Context, key string, item map[string]interface{}, deep bool) error {
	//log.Printf("Set: ID: %v - %+v\n", key, item)
	c.makeCachable(item)
	if err := c.first.Set(ctx, key, item); err != nil {
		return err
	}
	if deep && c.second != nil {
		return c.second.Set(ctx, key, item)
	}
	return nil
}

func (c *CacheWrapper) SetMulti(ctx context.Context, items map[string]map[string]interface{}, deep bool) error {
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
	if deep && c.second != nil {
		return c.second.SetMulti(ctx, cache)
	}
	return nil
}

func (c *CacheWrapper) Delete(ctx context.Context, key string, deep bool) error {
	if err := c.first.Delete(ctx, key); err != nil {
		return err
	}
	if deep && c.second != nil {
		return c.second.Delete(ctx, key)
	}
	return nil
}

func (c *CacheWrapper) DeleteMulti(ctx context.Context, keys []string, deep bool) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.first.DeleteMulti(ctx, keys); err != nil {
		return err
	}
	if deep && c.second != nil {
		return c.second.DeleteMulti(ctx, keys)
	}
	return nil
}

func (c *CacheWrapper) makeCachable(m map[string]interface{}) {
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
					if first.Kind() == reflect.Interface && first.Elem().Type() == reflect.TypeOf((*firestore.DocumentRef)(nil)) {
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

func (c *CacheWrapper) makeUnCachable(m map[string]interface{}) {
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
	if e, ok := getSessionCache(ctx)[key]; !ok {
		return CacheMiss
	} else {
		// set the value using reflection
		val := reflect.Indirect(reflect.ValueOf(v))
		val.Set(reflect.Indirect(reflect.ValueOf(e)))
		return nil
	}
}

/*
func (c *defaultCache) GetMulti(ctx context.Context, keys []string) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(keys))
	c.RLock()
	defer c.RUnlock()
	for _, k := range keys {
		if e, ok := getSessionCache(ctx)[k]; ok {
			result[k] = e
		}
	}
	return result, nil
}
*/
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
	if c, ok := ctx.Value(ContextKeySCache).(map[string]interface{}); ok {
		return c
	}
	//log.Println("ContextKeySCache is not found in context. Will use empty cache (no cache)")
	return make(map[string]interface{})
}
