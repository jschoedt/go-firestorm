package firestorm

import (
	"cloud.google.com/go/firestore"
	"context"
)

type contextKey string

func (c contextKey) String() string {
	return "context key " + string(c)
}

var (
	contextKeyTransaction = contextKey("transaction")
)

func getTransaction(ctx context.Context) (*firestore.Transaction, bool) {
	t, ok := ctx.Value(contextKeyTransaction).(*firestore.Transaction)
	return t, ok
}

func get(ctx context.Context, ref *firestore.DocumentRef) (*firestore.DocumentSnapshot, error) {
	if t, ok := getTransaction(ctx); ok {
		return t.Get(ref)
	}
	return ref.Get(ctx)
}

func getAll(ctx context.Context, client *firestore.Client, refs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
	if len(refs) == 0 {
		return []*firestore.DocumentSnapshot{}, nil
	}
	if t, ok := getTransaction(ctx); ok {
		return t.GetAll(refs)
	}
	return client.GetAll(ctx, refs)
}

func query(ctx context.Context, query firestore.Query) ([]*firestore.DocumentSnapshot, error) {
	if t, ok := getTransaction(ctx); ok {
		return t.Documents(query).GetAll()
	}
	return query.Documents(ctx).GetAll()
}

func create(ctx context.Context, ref *firestore.DocumentRef, m map[string]interface{}) error {
	if t, ok := getTransaction(ctx); ok {
		return t.Create(ref, m)
	}
	_, err := ref.Create(ctx, m)
	return err
}

func set(ctx context.Context, ref *firestore.DocumentRef, m map[string]interface{}) error {
	if t, ok := getTransaction(ctx); ok {
		return t.Set(ref, m)
	}
	_, err := ref.Set(ctx, m)
	return err
}

func del(ctx context.Context, ref *firestore.DocumentRef) error {
	if t, ok := getTransaction(ctx); ok {
		return t.Delete(ref)
	}
	_, err := ref.Delete(ctx)
	return err
}
