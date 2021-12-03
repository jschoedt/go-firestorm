package firestorm

import (
	"context"
	"errors"
	mapper "github.com/jschoedt/go-structmapper"
	"reflect"
	"strings"
	"time"
)

// DefaultToDBMapperFunc default mapper that maps entity fields and values to be firestore fields and values
func (fsc *FSClient) DefaultToDBMapperFunc(inKey string, inVal interface{}) (mt mapper.MappingType, outKey string, outVal interface{}) {
	inKey = strings.ToLower(inKey)
	// do not save as it is the id of the document
	switch inKey {
	case "id":
		return mapper.Ignore, inKey, inVal
	}

	v := reflect.ValueOf(inVal)
	switch v.Kind() {
	case reflect.Struct:
		if _, ok := inVal.(time.Time); ok {
			return mapper.Custom, inKey, inVal
		}
	case reflect.Slice:
		typ := v.Type().Elem()
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		// check if the type is an entity
		if v.Len() > 0 {
			first := v.Index(0)
			if fsc.IsEntity(first) {
				// make it interface type so it matches what firestore returns
				elemSlice := reflect.MakeSlice(reflect.TypeOf([]interface{}(nil)), v.Len(), v.Len())
				for i := 0; i < v.Len(); i++ {
					fromEmlPtr := v.Index(i)
					fromEmlValue := reflect.Indirect(fromEmlPtr)
					//log.Printf("val : %v", fromEmlValue)
					hid := fromEmlValue.Addr().Interface()
					toElmPtr := reflect.ValueOf(fsc.NewRequest().ToRef(hid))
					elemSlice.Index(i).Set(toElmPtr)
				}
				return mapper.Custom, inKey, elemSlice.Interface()
			}
		}
		return mapper.Default, inKey, inVal

	case reflect.Interface:
		fallthrough
	case reflect.Ptr:
		val := reflect.Indirect(v)
		if val.Kind() == reflect.Invalid {
			return mapper.Ignore, "", inVal // skip nil pointer
		}
		if fsc.IsEntity(val) {
			return mapper.Custom, inKey, fsc.NewRequest().ToRef(val.Interface())
		}
	}
	return mapper.Default, inKey, inVal
}

// DefaultFromDBMapperFunc default mapper that maps firestore fields and values to entity fields and values
func (fsc *FSClient) DefaultFromDBMapperFunc(inKey string, inVal interface{}) (mt mapper.MappingType, outKey string, outVal interface{}) {
	return mapper.Default, strings.Title(inKey), inVal
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
	err := fsc.MapFromDB.MapToStruct(m, p.Interface())

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
