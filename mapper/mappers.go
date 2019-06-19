package mapper

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// MapFunc used to map a field name and value to another field name and value
type MapFunc func(inKey string, inVal reflect.Value) (string, reflect.Value)

// Mapper used for mapping structs to maps or other structs
type Mapper struct {
	MapperFunc MapFunc
}

// New creates a new mapper
func New() *Mapper {
	m := &Mapper{DefaultMapFunk}
	return m
}

// NewWithFunc with an custom MapFunc
func NewWithFunc(mapperFunc MapFunc) *Mapper {
	m := &Mapper{mapperFunc}
	return m
}

// DefaultMapFunk default mapper that returns the same field name and value
func DefaultMapFunk(inKey string, inVal reflect.Value) (string, reflect.Value) {
	return inKey, inVal
}

// MapSlices maps a slice of structs to a slice of maps
func (mapper *Mapper) MapSlices(fromSlicePtr interface{}, toSlicePtr interface{}) error {
	fromValuePtr := reflect.ValueOf(fromSlicePtr)
	fromValue := reflect.Indirect(fromValuePtr)
	toValuePtr := reflect.ValueOf(toSlicePtr)
	toValue := reflect.Indirect(toValuePtr)
	c := make(map[interface{}]reflect.Value)

	switch fromValue.Kind() {
	case reflect.Slice:
		for i := 0; i < fromValue.Len(); i++ {
			fromEmlPtr := fromValue.Index(i)
			fromEmlValue := reflect.Indirect(fromEmlPtr)
			fromMap := flatten(fromEmlValue)
			toElmPtr := reflect.New(toValue.Type().Elem().Elem())
			mapper.mapMapToValues(fromMap, toElmPtr, c)
			toValue.Index(i).Set(toElmPtr)
		}
	}
	return nil
}

// MapTo takes a map or a struct ptr (as fromPtr) and maps to a struct ptr
func (mapper *Mapper) MapTo(fromPtr interface{}, toPtr interface{}) error {
	c := make(map[interface{}]reflect.Value)
	return mapper.cachedMapMapToStruct(fromPtr, toPtr, c)
}

func (mapper *Mapper) cachedMapMapToStruct(fromPtr interface{}, toPtr interface{}, c map[interface{}]reflect.Value) error {
	toStruct := reflect.Indirect(reflect.ValueOf(toPtr))
	fromStruct := reflect.Indirect(reflect.ValueOf(fromPtr))
	m, ok := fromPtr.(map[string]interface{})
	if ok {
		valMap := make(map[string]reflect.Value, len(m))
		for k, v := range m {
			valMap[strings.ToLower(k)] = reflect.ValueOf(v)
		}
		return mapper.mapMapToValues(valMap, toStruct, c)
	}
	fromMap := flatten(fromStruct)
	return mapper.mapMapToValues(fromMap, toStruct, c)
}

// MapStructToMap maps a struct to a map
func (mapper *Mapper) MapStructToMap(fromPtr interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	fromStruct := reflect.Indirect(reflect.ValueOf(fromPtr))
	fromMap := flatten(fromStruct)
	for k, v := range fromMap {
		k, v = mapper.MapperFunc(k, v)
		if k != "" {
			m[k] = v.Interface()
		}
	}
	return m
}

func (mapper *Mapper) mapMapToValues(fromMap map[string]reflect.Value, toPtr reflect.Value, c map[interface{}]reflect.Value) error {
	toStruct := reflect.Indirect(toPtr) // entity is a pointer
	toMap := flatten(toStruct)
	var errstrings []string

	//fmt.Printf("toMap: %v  \n", toMap)
	//fmt.Printf("fromMap: %v  \n", fromMap)
	for fromName, fromField := range fromMap {
		fromName, fromField = mapper.MapperFunc(fromName, fromField)
		if toField, ok := toMap[fromName]; ok {
			kind := fromField.Kind()
			if kind == reflect.Invalid {
				continue
			}

			// if same type just set it
			if fromField.Type().ConvertibleTo(toField.Type()) {
				setField(fromField, toField)
				continue
			}

			// convert the types
			switch kind {
			case reflect.Map: // try to map to object
				fromField = mapper.getFromValue(c, fromField, toField.Type())
			case reflect.Slice: // try to map to slice of objects
				if fromField.Len() == 0 {
					continue
				} else {
					elemSlice := reflect.MakeSlice(toField.Type(), fromField.Len(), fromField.Len())
					for i := 0; i < fromField.Len(); i++ {
						setField(mapper.getFromValue(c, fromField.Index(i), toField.Type().Elem()), elemSlice.Index(i))
					}
					fromField = elemSlice
				}
			}

			// try to set the value to to target after conversion
			if fromField.Type().ConvertibleTo(toField.Type()) {
				setField(fromField, toField)
			} else {
				errstrings = append(errstrings, fromName+":["+fromField.String()+" -> "+toField.String()+"]")
			}
		}
	}
	if len(errstrings) > 0 {
		return errors.New(strings.Join(errstrings, "\n"))
	}
	return nil
}

// Handles the creation of a value or a pointer to a value according to toType
func (mapper *Mapper) getFromValue(c map[interface{}]reflect.Value, fromField reflect.Value, toType reflect.Type) reflect.Value {
	var result reflect.Value
	//log.Printf("from: %v", reflect.TypeOf(fromField.Interface()))
	//log.Printf("to: %v", toType)
	if e, ok := c[fromField]; ok {
		result = e
	} else if reflect.TypeOf(fromField.Interface()).ConvertibleTo(toType) {
		return reflect.ValueOf(fromField.Interface())
	} else {
		if toType.Kind() == reflect.Map {
			result = reflect.MakeMapWithSize(toType, fromField.Len())
			for _, k := range fromField.MapKeys() {
				//fmt.Printf("from field: %v  \n", fromField.MapIndex(k))
				if fromField.MapIndex(k).Elem().Type().ConvertibleTo(toType.Elem()) {
					result.SetMapIndex(k, fromField.MapIndex(k).Elem().Convert(toType.Elem()))
				}
			}
			c[fromField] = result
		} else if toType.Kind() == reflect.Ptr {
			result = reflect.New(toType.Elem())
			c[fromField] = result
			mapper.cachedMapMapToStruct(fromField.Interface(), result.Interface(), c)
		} else {
			result = reflect.New(toType)
			c[fromField] = result
			mapper.cachedMapMapToStruct(fromField.Interface(), result.Interface(), c)
		}

	}
	if toType.Kind() == reflect.Ptr {
		return result
	}
	return reflect.Indirect(result)
}

func setField(fromField reflect.Value, toField reflect.Value) {
	if !toField.CanSet() {
		// now we can set unexported fields
		toField = reflect.NewAt(toField.Type(), unsafe.Pointer(toField.UnsafeAddr())).Elem()
	}
	toField.Set(fromField.Convert(toField.Type()))
}

func flatten(v reflect.Value) map[string]reflect.Value {
	//fmt.Printf("flatten: %v  \n", v)
	fields := make(map[string]reflect.Value, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		sf := v.Type().Field(i)

		switch f.Kind() {
		case reflect.Struct:
			if sf.Anonymous {
				embedFields := flatten(f)
				for k, v := range embedFields {
					fields[k] = v
				}
				break
			}
			fallthrough
		default:
			if !f.CanInterface() {
				// now we can get unexported fields
				//fmt.Printf("unexported field: %v  \n", sf.Name)
				f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
			}
			fields[strings.ToLower(sf.Name)] = f
		}
	}
	return fields
}

func getType(rv reflect.Value) reflect.Type {
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		fmt.Println(rv.Kind(), rv.Type())
		rv = rv.Elem()
	}
	return rv.Type()
}
