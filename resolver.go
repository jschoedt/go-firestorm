package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
	"strings"
)

// AllEntities loads all paths on the struct see: SetLoadPaths
const AllEntities = "ALL"

var refType = reflect.TypeOf((*firestore.DocumentRef)(nil))
var entityType = reflect.TypeOf((entityMap)(nil))

type entityMap = map[string]interface{}
type refSet = map[string]*firestore.DocumentRef
type resolveFunc func(m entityMap, ref *firestore.DocumentRef)

type refCollector struct {
	r                *resolver
	targetsToResolve map[string][]resolveFunc          // func that ads the result to the target
	refs             refSet                            // refs to resolve
	nfRefs           map[string]*firestore.DocumentRef // not found refs
}

func (r *resolver) NewRefCollector() *refCollector {
	return &refCollector{r, make(map[string][]resolveFunc), make(refSet), make(map[string]*firestore.DocumentRef)}
}

func (c *refCollector) Append(m entityMap, key string, ref *firestore.DocumentRef) {
	if e, ok := c.r.loaded[ref.Path]; ok {
		// it should be safe to modify although I think the spec is ambiguous
		// see: https://github.com/golang/go/issues/9926
		m[key] = e
	} else {
		resolveFunc := func(childM entityMap, childRef *firestore.DocumentRef) {
			m[key] = childM
		}
		c.targetsToResolve[ref.Path] = append(c.targetsToResolve[ref.Path], resolveFunc)
		c.refs[ref.Path] = ref
	}
}

func (c *refCollector) AppendSlice(m entityMap, key string, refs []*firestore.DocumentRef) {
	targetSlice := make([]entityMap, len(refs))
	// it should be safe to modify although I think the spec is ambiguous
	// see: https://github.com/golang/go/issues/9926
	m[key] = targetSlice
	for i, ref := range refs {
		if e, ok := c.r.loaded[ref.Path]; ok {
			targetSlice[i] = e
		} else {
			index := i // save index in closure
			resolveFunc := func(childM entityMap, childRef *firestore.DocumentRef) {
				targetSlice[index] = childM
			}
			c.targetsToResolve[ref.Path] = append(c.targetsToResolve[ref.Path], resolveFunc)
			c.refs[ref.Path] = ref
		}
	}
}

// resolves the elements matching the ref and removes them
func (c *refCollector) resolve(m entityMap, ref *firestore.DocumentRef) {
	if targets, ok := c.targetsToResolve[ref.Path]; ok {
		for _, target := range targets {
			target(m, ref)
		}
	}
}

func (c *refCollector) getRefs() []*firestore.DocumentRef {
	result := make([]*firestore.DocumentRef, 0, len(c.refs))
	for _, v := range c.refs {
		result = append(result, v)
	}
	return result
}

func (c *refCollector) AppendNotResolved(ref *firestore.DocumentRef) {
	c.nfRefs[ref.Path] = ref
}

func (c *refCollector) getErrors() error {
	if len(c.nfRefs) > 0 {
		return newNotFoundError(c.nfRefs)
	}
	return nil
}

type resolver struct {
	fsc      *FSClient
	resolved map[string]entityMap
	loaded   map[string]entityMap
	paths    []string
}

func newResolver(fsc *FSClient, paths ...string) *resolver {
	return &resolver{fsc, make(map[string]entityMap), make(map[string]entityMap), paths}
}

func (r *resolver) ResolveCacheRef(ctx context.Context, crefs []cacheRef) ([]entityMap, error) {
	result := make([]entityMap, len(crefs))
	r.Loaded(crefs)

	col := r.NewRefCollector()
	for i, cref := range crefs {
		m := cref.GetResult()
		if len(m) != 0 {
			r.resolveEntity(m, cref.Ref, col, r.paths...)
			result[i] = m
		} else {
			col.AppendNotResolved(cref.Ref)
		}
	}

	if err := r.resolveChildren(ctx, col, r.paths...); err != nil {
		return nil, err
	}

	return result, col.getErrors()
}

func (r *resolver) ResolveDocs(ctx context.Context, docs []*firestore.DocumentSnapshot) ([]entityMap, error) {
	result := make([]entityMap, len(docs))
	col := r.NewRefCollector()
	for i, doc := range docs {
		if doc.Exists() {
			m := doc.Data()
			r.resolveEntity(m, doc.Ref, col, r.paths...)
			result[i] = m
		} else {
			col.AppendNotResolved(doc.Ref)
		}
	}

	if err := r.resolveChildren(ctx, col, r.paths...); err != nil {
		return nil, err
	}

	return result, col.getErrors()
}

func (r *resolver) resolveChildren(ctx context.Context, col *refCollector, paths ...string) error {
	// base case stop recursion when no more children are present
	refs := col.getRefs()
	if len(refs) == 0 {
		return nil
	}

	// cut off the first path in the paths list
	nextPaths := make([]string, 0, len(paths))
	for _, v := range paths {
		split := strings.Split(v, ".")
		if len(split) > 1 {
			nextPaths = append(nextPaths, split[1])
		}
	}

	// now query the DB
	crefs, err := r.fsc.getCachedEntities(ctx, refs)
	if err != nil {
		return err
	}
	r.Loaded(crefs)
	childCol := r.NewRefCollector()
	for _, cref := range crefs {
		result := cref.GetResult()
		if len(result) == 0 { // add not found refs
			col.AppendNotResolved(cref.Ref)
			continue
		}
		r.resolveEntity(result, cref.Ref, childCol, nextPaths...)
		col.resolve(result, cref.Ref)
	}
	return r.resolveChildren(ctx, childCol, nextPaths...)
}

func (r *resolver) resolveEntity(m entityMap, ref *firestore.DocumentRef, col *refCollector, paths ...string) {
	// only resolve it once
	if ref != nil {
		if _, ok := r.resolved[ref.Path]; ok {
			return
		}
		r.resolved[ref.Path] = m
		m[r.fsc.IDKey] = ref.ID
		//m["createtime"] = doc.CreateTime
		//m["updatetime"] = doc.UpdateTime
		//m["readtime"] = doc.ReadTime
	}

	for k, v := range m {
		switch val := v.(type) {
		case *firestore.DocumentRef:
			if r.contains(k, paths...) {
				col.Append(m, k, val)
			} else {
				delete(m, k)
			}
		default:
			valOf := reflect.ValueOf(v)
			switch valOf.Kind() {
			case reflect.Map:
				if valOf.Len() > 0 && valOf.Type() == entityType {
					r.resolveEntity(v.(entityMap), nil, col, paths...)
				}
			case reflect.Slice:
				if valOf.Len() > 0 {
					first := valOf.Index(0)

					// from firestore the type of slice is interface
					if first.Kind() == reflect.Interface {
						first = first.Elem()
					}
					//fmt.Printf("kind: %v type: %v  \n", first.Kind(), first.Type())

					if first.Kind() == reflect.Map {
						for i := 0; i < valOf.Len(); i++ {
							r.resolveEntity(valOf.Index(i).Interface().(entityMap), nil, col, paths...)
						}
					} else if first.Type() == refType {
						if !r.contains(k, paths...) {
							delete(m, k)
							continue
						}
						refs := make([]*firestore.DocumentRef, valOf.Len())
						for i := 0; i < valOf.Len(); i++ {
							fromEmlPtr := valOf.Index(i)
							refs[i] = fromEmlPtr.Interface().(*firestore.DocumentRef)
						}
						col.AppendSlice(m, k, refs)
					}

				}
			}
		}
	}
}

func (r *resolver) contains(find string, paths ...string) bool {
	if find == r.fsc.ParentKey {
		return true
	}
	for _, a := range paths {
		if a == AllEntities || strings.Index(a, find) == 0 {
			return true
		}
	}
	return false
}

func (r *resolver) Loaded(refs []cacheRef) {
	for _, v := range refs {
		r.loaded[v.Ref.Path] = v.GetResult()
	}
}
