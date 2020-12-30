package firestormtests

import (
	"context"
	"firebase.google.com/go"
	"github.com/jschoedt/go-firestorm"
	"google.golang.org/api/option"
	"io/ioutil"
	"testing"
)

var fsc *firestorm.FSClient

func init() {
	ctx := context.Background()

	b, _ := ioutil.ReadFile("../auth/sn-dev.json")

	app, _ := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(b))

	dbClient, _ := app.Firestore(ctx)

	fsc = firestorm.New(dbClient, "ID", "")
}

func testRunner(t *testing.T, f func(ctx context.Context, t *testing.T)) {
	f(context.Background(), t)
	f(createSessionCacheContext(), t)
}

func cleanup(entities ...interface{}) {
	fsc.NewRequest().DeleteEntities(context.Background(), entities)()
}

func createSessionCacheContext() context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, firestorm.SessionCacheKey, make(map[string]interface{}))
}

func getSessionCache(ctx context.Context) map[string]interface{} {
	if c, ok := ctx.Value(firestorm.SessionCacheKey).(map[string]interface{}); ok {
		return c
	}
	return nil
}
