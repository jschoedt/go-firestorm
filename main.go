package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	mapper "github.com/jschoedt/go-structmapper"
)

// FSClient is the client used to perform the CRUD actions
type FSClient struct {
	Client           *firestore.Client
	MapToDB          *mapper.Mapper
	MapFromDB        *mapper.Mapper
	IDKey, ParentKey string
	Cache            *cacheWrapper
	IsEntity         func(i interface{}) bool
}

// NewRequest creates a new CRUD Request to firestore
func (fsc *FSClient) NewRequest() *Request {
	r := &Request{}
	r.FSC = fsc
	r.mapperFunc = func(i map[string]interface{}) {
		return
	}
	return r
}

// New creates a firestorm client. Supply the names of the id and parent fields of your model structs
// Leave parent blank if sub-collections are not used.
func New(client *firestore.Client, id, parent string) *FSClient {
	c := &FSClient{}
	c.Client = client
	c.MapToDB = mapper.New()
	c.MapToDB.MapFunc = c.DefaultToDBMapperFunc
	c.MapFromDB = mapper.New()
	c.MapFromDB.MapFunc = c.DefaultFromDBMapperFunc
	c.IDKey = id
	c.ParentKey = parent
	c.Cache = newCacheWrapper(client, newDefaultCache(), nil)
	c.IsEntity = isEntity(c.IDKey)
	return c
}

// SetCache sets a second level cache besides the session cache. Use it for eg. memcache or redis
func (fsc *FSClient) SetCache(cache Cache) {
	fsc.Cache = newCacheWrapper(fsc.Client, newDefaultCache(), cache)
}

// getCache gets the transaction cache when inside a transaction - otherwise the global cache
func (fsc *FSClient) getCache(ctx context.Context) *cacheWrapper {
	if c, ok := ctx.Value(transCacheKey).(*cacheWrapper); ok {
		return c
	}
	return fsc.Cache
}

// isEntity tests if the i is a firestore entity
func isEntity(id string) func(i interface{}) bool {
	return func(i interface{}) bool {
		_, err := getIDValue(id, i)
		return err == nil
	}
}
