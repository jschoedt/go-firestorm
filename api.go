package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"sync"
)

// DoInTransaction wraps any updates that needs to run in a transaction.
// Use the f Context for any calls that need to be part of the transaction.
// Do reads before writes as required by firestore
func (fsc *FSClient) DoInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	err := fsc.Client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		m := make(map[string]interface{})
		tctx := context.WithValue(ctx, contextKeyTransaction, t)
		tctx = context.WithValue(tctx, ContextKeySCache, m)
		result := f(tctx)
		// update parent cache by removing entities in used
		for k := range m {
			fsc.Cache.Delete(ctx, k, true)
		}
		return result
	})
	if err == nil {

	}
	return err
}

func (fsc *FSClient) getEntities(ctx context.Context, req *Request, sliceVal reflect.Value) func() ([]interface{}, error) {
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

		resolver := newResolver(fsc, req.loadPaths...)
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

func (fsc *FSClient) getCachedEntities(ctx context.Context, refs []*firestore.DocumentRef) ([]cacheRef, error) {
	res := make([]cacheRef, len(refs))
	load := make([]*firestore.DocumentRef, 0, len(refs))

	// check cache and collect refs not loaded yet
	for i, ref := range refs {
		if e, err := fsc.Cache.Get(ctx, ref, true); err == nil {
			res[i] = e // we found it
		} else {
			if err != ErrCacheMiss {
				log.Printf("Cache error but continue: %+v", err)
			}
			load = append(load, ref)
		}
	}

	// get the unloaded refs
	docs, err := getAll(ctx, fsc.Client, load)
	if err != nil {
		return nil, err
	}

	// fill the res slice with the DB results
	i := 0
	multi := make(map[string]map[string]interface{}, len(docs))
	for _, doc := range docs {
		ref := newCacheRef(doc.Data(), doc.Ref)
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

func (fsc *FSClient) queryEntities(ctx context.Context, req *Request, p firestore.Query, toSlicePtr interface{}) FutureFunc {
	asyncFunc := func() error {
		docs, err := query(ctx, p)
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
		resolver := newResolver(fsc, req.loadPaths...)
		res, err := resolver.ResolveDocs(ctx, docs)
		if err != nil {
			return err
		}
		return fsc.toEntities(ctx, res, toSlicePtr)
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) createEntity(ctx context.Context, req *Request, entity interface{}) FutureFunc {
	asyncFunc := func() error {
		m, err := fsc.MapToDB.MapStructToMap(entity)
		if err != nil {
			return err
		}

		ref := req.ToRef(entity)
		// if we need a fixed ID use that
		if req.GetID(entity) == "" {
			ref = req.ToCollection(entity).NewDoc() // otherwise create new id
			req.SetID(entity, ref.ID)
		}
		req.mapperFunc(m)
		if err := create(ctx, ref, m); err != nil {
			return err
		}
		if err := fsc.Cache.Set(ctx, ref.Path, m, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) createEntities(ctx context.Context, req *Request, sliceVal reflect.Value) FutureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]FutureFunc, slice.Len())
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

func (fsc *FSClient) updateEntity(ctx context.Context, req *Request, entity interface{}) FutureFunc {
	asyncFunc := func() error {
		m, err := fsc.MapToDB.MapStructToMap(entity)
		if err != nil {
			return err
		}

		ref := req.ToRef(entity)
		req.mapperFunc(m)
		if err := set(ctx, ref, m); err != nil {
			return err
		}
		if err := fsc.Cache.Set(ctx, ref.Path, m, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) updateEntities(ctx context.Context, req *Request, sliceVal reflect.Value) FutureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]FutureFunc, slice.Len())
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

func (fsc *FSClient) deleteEntity(ctx context.Context, req *Request, entity interface{}) FutureFunc {
	asyncFunc := func() error {
		ref := req.ToRef(entity)
		if err := del(ctx, ref); err != nil {
			return err
		}
		if err := fsc.Cache.Delete(ctx, ref.Path, true); err != nil {
			log.Printf("Cache error but continue: %+v", err)
		}
		return nil
	}
	return runAsync(ctx, asyncFunc)
}

func (fsc *FSClient) deleteEntities(ctx context.Context, req *Request, sliceVal reflect.Value) FutureFunc {
	asyncFunc := func() error {
		slice := sliceVal
		futures := make([]FutureFunc, slice.Len())
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

// FutureFunc is a function that when called blocks until the result is ready
type FutureFunc func() error

func runAsync(ctx context.Context, fun asyncFunc) FutureFunc {
	if _, ok := getTransaction(ctx); ok {
		// transactions are not thread safe so just execute the func
		//==================
		//WARNING: DATA RACE
		//Read at 0x00c0004bde90 by goroutine 99:
		//  cloud.google.com/go/firestore.(*Transaction).addWrites()
		//      /home/jens/go/pkg/mod/cloud.google.com/go@v0.28.0/firestore/transaction.go:270 +0x124
		return FutureFunc(fun)
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
