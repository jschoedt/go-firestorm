package crud

import (
	"cloud.google.com/go/firestore"
	"context"
)

type contextKey string

func (c contextKey) String() string {
	return "context key " + string(c)
}

var (
	ContextKeyTransaction = contextKey("transaction")
)

func GetTransaction(ctx context.Context) (*firestore.Transaction, bool) {
	t, ok := ctx.Value(ContextKeyTransaction).(*firestore.Transaction)
	return t, ok
}

func Get(ctx context.Context, ref *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
	if t, ok := GetTransaction(ctx); ok {
		return t.Get(ref)
	}
	return ref.Get(ctx)
}

func GetAll(ctx context.Context, client *firestore.Client, refs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
	if len(refs) == 0 {
		return []*firestore.DocumentSnapshot{}, nil
	}
	if t, ok := GetTransaction(ctx); ok {
		return t.GetAll(refs)
	}
	return client.GetAll(ctx, refs)
}

func Query(ctx context.Context, query firestore.Query) ([]*firestore.DocumentSnapshot, error) {
	if t, ok := GetTransaction(ctx); ok {
		return t.Documents(query).GetAll()
	}
	return query.Documents(ctx).GetAll()
}

func Create(ctx context.Context, ref *firestore.DocumentRef, m map[string]interface{}) error {
	if t, ok := GetTransaction(ctx); ok {
		return t.Create(ref, m)
	}
	_, err := ref.Create(ctx, m)
	return err
}

func Set(ctx context.Context, ref *firestore.DocumentRef, m map[string]interface{}) error {
	if t, ok := GetTransaction(ctx); ok {
		return t.Set(ref, m)
	}
	_, err := ref.Set(ctx, m)
	return err
}

func Del(ctx context.Context, ref *firestore.DocumentRef) error {
	if t, ok := GetTransaction(ctx); ok {
		return t.Delete(ref)
	}
	_, err := ref.Delete(ctx)
	return err
}
