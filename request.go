package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Request a request builder for querying firestore
type Request struct {
	FSC        *FSClient
	loadPaths  []string
	mapperFunc mapperFunc
}

type mapperFunc func(map[string]interface{})

// SetMapperFunc is called before the map is saved to firestore.
// This can be used to modify the map before it is saved
func (req *Request) SetMapperFunc(mapperFunc mapperFunc) *Request {
	req.mapperFunc = mapperFunc
	return req
}

// SetLoadPaths adds the paths (refs) to load for the entity.
// Eg. to load a users grandmother: 'mother.mother'
// To load all refs on the struct use firestorm.AllEntities
// See examples: https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go
func (req *Request) SetLoadPaths(paths ...string) *Request {
	req.loadPaths = paths
	return req
}

// ToCollection creates a firestore CollectionRef to the entity
func (req *Request) ToCollection(entity interface{}) *firestore.CollectionRef {
	path := getTypeName(entity)

	// prefix any parents
	for p := req.GetParent(entity); p != nil; p = req.GetParent(p) {
		n := getTypeName(p)
		path = n + "/" + req.GetID(p) + "/" + path
	}

	return req.FSC.Client.Collection(path)
}

// GetParent gets the patent of the entity
func (req *Request) GetParent(entity interface{}) interface{} {
	v, err := getIDValue(req.FSC.ParentKey, entity)
	if err != nil {
		return nil
	}
	return v.Interface()
}

// GetID gets the id of the entity. It panics if the entity does not have an ID field.
func (req *Request) GetID(entity interface{}) string {
	if v, err := getIDValue(req.FSC.IDKey, entity); err != nil {
		panic(err)
	} else {
		return v.Interface().(string)
	}
}

func getIDValue(id string, entity interface{}) (reflect.Value, error) {
	v := reflect.ValueOf(entity)
	if cv, ok := entity.(reflect.Value); ok {
		v = cv
	}
	v = reflect.Indirect(v)
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		sf := v.Type().Field(i)

		switch f.Kind() {
		case reflect.Struct:
			if sf.Anonymous {
				if sv, err := getIDValue(id, f); err == nil {
					return sv, nil
				}
			}
		}

		// first check if id is statically set
		if strings.ToLower(sf.Name) == id {
			return f, nil
		}
		// otherwise use the tag
		/* not supported yet
		if tag, ok := sf.Tag.Lookup("firestorm"); ok {
			if tag == "id" {
				return f, nil
			}
		}
		*/
	}
	return v, fmt.Errorf("entity has no id field defined: %v", entity)
}

// SetID sets the id field to the given id
func (req *Request) SetID(entity interface{}, id string) {
	v, err := getIDValue(req.FSC.IDKey, entity)
	if err != nil {
		panic(err)
	}
	v.SetString(id)
}

// ToRef creates a firestore DocumentRef for the entity
func (req *Request) ToRef(entity interface{}) *firestore.DocumentRef {
	return req.ToCollection(entity).Doc(req.GetID(entity))
}

// GetEntities reads the entities from the database by their id. Supply either a pointer to a struct or pointer to a slice. Returns a
// slice containing the found entities and an error if some entities are not found.
func (req *Request) GetEntities(ctx context.Context, entities interface{}) func() ([]interface{}, error) {
	v := reflect.Indirect(reflect.ValueOf(entities))
	switch v.Kind() {
	case reflect.Struct:
		v = reflect.ValueOf([]interface{}{entities})
		fallthrough
	case reflect.Slice:
		return req.FSC.getEntities(ctx, req, v)
	}
	return func() (i []interface{}, e error) {
		return nil, fmt.Errorf("kind not supported: %s", v.Kind().String())
	}
}

// CreateEntities creates the entities and auto creates the id if left empty. Supply either a struct or a slice
// as value or reference.
func (req *Request) CreateEntities(ctx context.Context, entities interface{}) FutureFunc {
	v := reflect.Indirect(reflect.ValueOf(entities))
	switch v.Kind() {
	case reflect.Struct:
		return req.FSC.createEntity(ctx, req, entities)
	case reflect.Slice:
		return req.FSC.createEntities(ctx, req, v)
	}
	return createErrorFunc(fmt.Sprintf("kind not supported: %s", v.Kind().String()))
}

// UpdateEntities updates the entities. Supply either a struct or a slice
// as value or reference.
func (req *Request) UpdateEntities(ctx context.Context, entities interface{}) FutureFunc {
	v := reflect.Indirect(reflect.ValueOf(entities))
	switch v.Kind() {
	case reflect.Struct:
		return req.FSC.updateEntity(ctx, req, entities)
	case reflect.Slice:
		return req.FSC.updateEntities(ctx, req, v)
	}
	return createErrorFunc(fmt.Sprintf("Kind not supported: %s", v.Kind().String()))
}

// DeleteEntities deletes the entities. Supply either a struct or a slice
// as value or reference.
func (req *Request) DeleteEntities(ctx context.Context, entities interface{}) FutureFunc {
	v := reflect.Indirect(reflect.ValueOf(entities))
	switch v.Kind() {
	case reflect.Struct:
		return req.FSC.deleteEntity(ctx, req, entities)
	case reflect.Slice:
		return req.FSC.deleteEntities(ctx, req, v)
	}
	return createErrorFunc(fmt.Sprintf("Kind not supported: %s", v.Kind().String()))
}

// QueryEntities query for entities. Supply a reference to a slice for the result
func (req *Request) QueryEntities(ctx context.Context, query firestore.Query, toSlicePtr interface{}) FutureFunc {
	return req.FSC.queryEntities(ctx, req, query, toSlicePtr)
}

func createErrorFunc(s string) func() error {
	return func() error {
		return errors.New(s)
	}
}
