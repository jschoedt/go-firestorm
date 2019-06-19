package firestormtests

import (
	"context"
	"firebase.google.com/go"
	"github.com/jschoedt/go-firestorm"
	"google.golang.org/api/option"
	"io/ioutil"
)

var fsc *firestorm.FSClient
var ctx = context.Background()

func init() {
	b, _ := ioutil.ReadFile("../auth/sn-dev.json")

	app, _ := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(b))

	dbClient, _ := app.Firestore(ctx)

	fsc = firestorm.New(dbClient, "ID", "")
}
