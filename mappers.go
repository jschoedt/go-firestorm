package firestorm

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"cloud.google.com/go/firestore"
)

// DefaultToDBMapperFunc default mapper that maps entity fields and values to be firestore fields and values
func (fsc *FSClient) DefaultToDBMapperFunc(inKey string, inVal reflect.Value) (string, reflect.Value) {
	// do not save as it is the id of the document
	switch inKey {
	case "id":
		return "", inVal
	}

	switch inVal.Kind() {
	case reflect.Slice:
		typ := inVal.Type().Elem()
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		// check if the type is an entity
		elemType := reflect.TypeOf((*firestore.DocumentRef)(nil))
		elemSlice := reflect.MakeSlice(reflect.SliceOf(elemType), inVal.Len(), inVal.Len())
		for i := 0; i < inVal.Len(); i++ {
			fromEmlPtr := inVal.Index(i)
			fromEmlValue := reflect.Indirect(fromEmlPtr)
			//log.Printf("val : %v", fromEmlValue)
			if fromEmlValue.Kind() != reflect.Struct || !fsc.IsEntity(fromEmlValue) {
				return inKey, inVal
			}
			hid := fromEmlValue.Addr().Interface()
			toElmPtr := reflect.ValueOf(fsc.NewRequest().ToRef(hid))
			elemSlice.Index(i).Set(toElmPtr)
		}
		return inKey, elemSlice
	case reflect.Interface:
		fallthrough
	case reflect.Ptr:
		val := reflect.Indirect(inVal)
		if val.Kind() == reflect.Invalid {
			return "", inVal // skip nil pointer
		}
		if fsc.IsEntity(val) {
			return inKey, reflect.ValueOf(fsc.NewRequest().ToRef(val.Interface()))
		}
	}
	return inKey, inVal
}

// DefaultFromDBMapperFunc default mapper that maps firestore fields and values to entity fields and values
func (fsc *FSClient) DefaultFromDBMapperFunc(inKey string, inVal reflect.Value) (s string, value reflect.Value) {
	return inKey, inVal
}

func (fsc *FSClient) toEntities(ctx context.Context, entities []entityMap, toSlicePtr interface{}) error {
	var errs []string
	valuePtr := reflect.ValueOf(toSlicePtr)
	value := reflect.Indirect(valuePtr)
	for _, m := range entities {
		// log.Printf("type %v", value.Type().Elem())
		if p, err := fsc.toEntity(ctx, m, value.Type().Elem()); err != nil {
			errs = append(errs, err.Error())
			continue
		} else {
			value.Set(reflect.Append(value, p))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func (fsc *FSClient) toEntity(ctx context.Context, m map[string]interface{}, typ reflect.Type) (reflect.Value, error) {
	isPtr := typ.Kind() == reflect.Ptr
	if isPtr {
		typ = typ.Elem()
	}

	p := reflect.New(typ)
	err := fsc.MapFromDB.MapTo(m, p.Interface())

	if isPtr {
		return p, err
	}
	return reflect.Indirect(p), err
}

func getTypeName(i interface{}) string {
	return getStructType(i).Name()
}

func getStructType(i interface{}) reflect.Type {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}
