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
	// make a copy so modifications to the result map does not modify the original map
	res := make(map[string]interface{}, len(result))
	for k, e := range result {
		res[k] = e
	}
	return cacheRef{result: res, Ref: ref}
}
