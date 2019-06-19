package firestorm

import (
	"cloud.google.com/go/firestore"
	"github.com/jschoedt/go-firestorm/mapper"
	"strings"
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
	c.MapToDB.MapperFunc = c.DefaultToDBMapperFunc
	c.MapFromDB = mapper.New()
	c.MapFromDB.MapperFunc = c.DefaultFromDBMapperFunc
	c.IDKey = strings.ToLower(id)
	c.ParentKey = strings.ToLower(parent)
	c.Cache = newCacheWrapper(client, newDefaultCache(), nil)
	c.IsEntity = isEntity(c.IDKey)
	return c
}

func isEntity(id string) func(i interface{}) bool {
	return func(i interface{}) bool {
		_, err := getIDValue(id, i)
		return err == nil
	}
}
