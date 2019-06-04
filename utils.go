package firestorm

import (
	"cloud.google.com/go/firestore"
)

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
