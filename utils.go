package firestorm

import (
	"cloud.google.com/go/firestore"
)

type CacheRef struct {
	result map[string]interface{}
	Ref    *firestore.DocumentRef
}

func (ref CacheRef) GetResult() map[string]interface{} {
	return ref.result
}

func NewCacheRef(result map[string]interface{}, ref *firestore.DocumentRef) CacheRef {
	return CacheRef{result: result, Ref: ref}
}
