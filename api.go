package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"github.com/jschoedt/go-firestorm/crud"
	"log"
	"reflect"
	"strings"
	"sync"
)

func (fsc *FSClient) DoInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	err := fsc.Client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		// TODO: add a new cache to context
		m := make(map[string]interface{})
		tctx := context.WithValue(ctx, crud.ContextKeyTransaction, t)
		tctx = context.WithValue(tctx, ContextKeySCache, m)
		result := f(tctx)
		// update parent cache
		return result
	})
	if err == nil {

	}
	return err
}

func (fsc *FSClient) getEntities(ctx context.Context, req *request, sliceVal reflect.Value) func() ([]interface{}, error) {
	slice := sliceVal
	result := make([]interface{}, 0, slice.Len())
	asyncFunc := func() error {
		var nfErr error
		refs := make([]*firestore.DocumentRef, slice.Len())
		for i := 0; i < slice.Len(); i++ {
			refs[i] = req.ToRef(slice.Index(i).Interface())
		}
		crefs, err := fsc.getCachedEntities(ctx, refs)
		if err != nil {
			return err
		}

		resolver := NewResolver(fsc, req.loadPaths...)
		res, err := resolver.ResolveCacheRef(ctx, crefs)

		if err != nil {
			if err, ok := err.(NotFoundError); ok {
				nfErr = err
			} else {
				return err
			}
		}

		for i, v := range res {
			if len(v) > 0 {
				fsc.MapFromDB.MapTo(v, slice.Index(i).Interface())
				result = append(result, slice.Index(i).Interface())
			}
		}
		return nfErr
	}
	af := runAsync(ctx, asyncFunc)
	return func() (entities []interface{}, e error) {
		err := af()
		return result, err
	}
}

func (fsc *FSClient) getCachedEntities(ctx context.Context, refs []*firestore.DocumentRef) ([]CacheRef, error) {
	res := make([]CacheRef, len(refs))
	load := make([]*firestore.DocumentRef, 0, len(refs))

	// check cache and collect refs not loaded yet
	for i, ref := range refs {
		if e, err := fsc.Cache.Get(ctx, ref, true); err == nil {
			res[i] = e // we found it
		} else {
			if err != CacheMiss {
				log.Printf("Cache error but continue: %+v", err)
			}
			load = append(load, ref)
		}
	}

	// get the unloaded refs
	docs, err := crud.GetAll(ctx, fsc.Client, load)
	if err != nil {
		return nil, err
	}

	// fill the res slice with the DB results
	i := 0
	multi := make(map[string]map[string]interface{}, len(docs))
	for _, doc := range docs {
		ref := NewCacheRef(doc.Data(), doc.Ref)
		multi[doc.Ref.Path] = doc.Data()
		for _, v := range res[i:] {
			if v.Ref == nil {
				res[i] = ref
				i++
				break
			}
			i++
		}
	}
	if err = fsc.Cache.SetMulti(ctx, multi, true); err != nil {
		log.Printf("Cache error but continue: %+v", err)
	}
	return res, nil
}

func (fsc *FSClient) queryEntities(ctx context.Context, req *request, query firestore.Query, toSlicePtr interface{}) futureFunc {
	asyncFunc := func() error {
		docs, err := crud.Query(ctx, query)
		if err != nil {
			return err
		}
		multi := make(map[string]map[string]interface{}, len(docs))
		for _, doc := range docs {
			multi[doc.Ref.Path] = doc.Data()
		}
		if err = fsc.Cache.SetMulti(ctx, multi, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		resolver := NewResolver(fsc, req.loadPaths...)
		if res, err := resolver.ResolveDocs(ctx, docs); err != nil {
			return err
		} else {
			return fsc.toEntities(ctx, res, toSlicePtr)
		}
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) createEntity(ctx context.Context, req *request, entity interface{}) futureFunc {
	asyncFunc := func() error {
		m := fsc.MapToDB.MapStructToMap(entity)

		ref := req.ToRef(entity)
		// if we need a fixed ID use that
		if req.GetID(entity) == "" {
			ref = req.ToCollection(entity).NewDoc() // otherwise create new id
			req.SetID(entity, ref.ID)
		}
		req.mapperFunc(m)
		if err := crud.Create(ctx, ref, m); err != nil {
			return err
		}
		if err := fsc.Cache.Set(ctx, ref.Path, m, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) createEntities(ctx context.Context, req *request, sliceVal reflect.Value) futureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]futureFunc, slice.Len())
		var errs []string

		// kick off all updates and collect futures
		for i := 0; i < slice.Len(); i++ {
			futures[i] = fsc.createEntity(ctx, req, slice.Index(i).Interface())
		}

		// wait for all futures to finish
		for _, f := range futures {
			if err := f(); err != nil {
				errs = append(errs, err.Error())
			}
		}

		// check the errors
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "\n"))
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) updateEntity(ctx context.Context, req *request, entity interface{}) futureFunc {
	asyncFunc := func() error {
		m := fsc.MapToDB.MapStructToMap(entity)

		ref := req.ToRef(entity)
		req.mapperFunc(m)
		if err := crud.Set(ctx, ref, m); err != nil {
			return err
		}
		if err := fsc.Cache.Set(ctx, ref.Path, m, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) updateEntities(ctx context.Context, req *request, sliceVal reflect.Value) futureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]futureFunc, slice.Len())
		var errs []string

		// kick off all updates and collect futures
		for i := 0; i < slice.Len(); i++ {
			futures[i] = fsc.updateEntity(ctx, req, slice.Index(i).Interface())
		}

		// wait for all futures to finish
		for _, f := range futures {
			if err := f(); err != nil {
				errs = append(errs, err.Error())
			}
		}

		// check the errors
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "\n"))
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) deleteEntity(ctx context.Context, req *request, entity interface{}) futureFunc {
	asyncFunc := func() error {
		ref := req.ToRef(entity)
		if err := crud.Del(ctx, ref); err != nil {
			return err
		}
		if err := fsc.Cache.Delete(ctx, ref.Path, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) deleteEntities(ctx context.Context, req *request, sliceVal reflect.Value) futureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]futureFunc, slice.Len())
		var errs []string

		// kick off all updates and collect futures
		for i := 0; i < slice.Len(); i++ {
			futures[i] = fsc.deleteEntity(ctx, req, slice.Index(i).Interface())
		}

		// wait for all futures to finish
		for _, f := range futures {
			if err := f(); err != nil {
				errs = append(errs, err.Error())
			}
		}

		// check the errors
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "\n"))
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

type asyncFunc func() error
type futureFunc func() error

func runAsync(ctx context.Context, fun asyncFunc) futureFunc {
	if _, ok := crud.GetTransaction(ctx); ok {
		// transactions are not thread safe so just execute the func
		//==================
		//WARNING: DATA RACE
		//Read at 0x00c0004bde90 by goroutine 99:
		//  cloud.google.com/go/firestore.(*Transaction).addWrites()
		//      /home/jens/go/pkg/mod/cloud.google.com/go@v0.28.0/firestore/transaction.go:270 +0x124
		return futureFunc(fun)
	}

	var err error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err = fun()
	}()

	return func() error {
		wg.Wait()
		return err
	}
}
