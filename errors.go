package firestorm

import (
	"cloud.google.com/go/firestore"
	"fmt"
)

type NotFoundError struct {
	Refs map[string]*firestore.DocumentRef
}

func NewNotFoundError(refs map[string]*firestore.DocumentRef) NotFoundError {
	return NotFoundError{refs}
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("Not found error %v", e.Refs)
}
