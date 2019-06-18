package firestorm

import (
	"cloud.google.com/go/firestore"
	"fmt"
)

// NotFoundError is returned when any of the entities are not found in firestore
// The error can be ignored if dangling references is not a problem
type NotFoundError struct {
	// Refs contains the references not found
	Refs map[string]*firestore.DocumentRef
}

func newNotFoundError(refs map[string]*firestore.DocumentRef) NotFoundError {
	return NotFoundError{refs}
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("Not found error %v", e.Refs)
}
