package firestorm

import (
	"cloud.google.com/go/firestore"
)

type EntityMap map[string]interface{}

// Copy returns a 'shallow' copy of the map.
func (entity EntityMap) Copy() EntityMap {
	if entity == nil {
		return entity
	}
	m := make(EntityMap, len(entity))
	for k, v := range entity {
		m[k] = v
	}
	return m
}

type cacheRef struct {
	result map[string]interface{}
	Ref    *firestore.DocumentRef
}

func (ref cacheRef) GetResult() map[string]interface{} {
	return ref.result
}

func newCacheRef(result map[string]interface{}, ref *firestore.DocumentRef) cacheRef {
	return cacheRef{result: result, Ref: ref}
}
