[![pipeline status](https://gitlab.com/jens.schoedt/go-firestorm/badges/master/pipeline.svg)](https://gitlab.com/jens.schoedt/go-firestorm/commits/master)
[![coverage report](https://gitlab.com/jens.schoedt/go-firestorm/badges/master/coverage.svg)](https://gitlab.com/jens.schoedt/go-firestorm/commits/master)
[![Go Report Card](https://goreportcard.com/badge/github.com/jschoedt/go-firestorm)](https://goreportcard.com/report/github.com/jschoedt/go-firestorm)
[![GoDoc](https://godoc.org/github.com/jschoedt/go-firestorm?status.svg)](https://godoc.org/github.com/jschoedt/go-firestorm)
[![GitHub](https://img.shields.io/github/license/jschoedt/go-firestorm)](https://github.com/jschoedt/go-firestorm/blob/master/LICENSE)

# go-firestorm
Go ORM ([Object-relational mapping](https://en.wikipedia.org/wiki/Object-relational_mapping)) for [Google Cloud Firestore](https://cloud.google.com/firestore/).

#### Goals
1. Easy to use
2. Non-intrusive
4. Non-exclusive
3. Fast

#### Features
- Basic CRUD operations
- Search
- Concurrent requests support (except when run in transactions)
- Transactions
- Nested transactions will reuse the first transaction (reads before writes as required by firestore)
- Configurable auto load of references
- Handles cyclic references
- Sub collections
- Supports embedded/anonymous structs
- Supports unexported fields
- Custom mappers between fields and types
- Caching (session + second level)
- Supports Google App Engine - 2. Gen (go version >= 1.11)


## Getting Started

* [Prerequisites](#prerequisites)
* [Basic CRUD example](#basic-crud-example)
* [Search](#search)
* [Concurrent requests](#concurrent-requests)
* [Transactions](#transactions)
* [Cache](#cache)
* [Configurable auto load of references](#configurable-auto-load-of-references)
* [Help](#help)


#### Prerequisites

This library only supports Firestore Native mode and not the old [Datastore](https://cloud.google.com/datastore/docs/firestore-or-datastore) mode.
```
go get -u github.com/jschoedt/go-firestorm
```

#### Setup

1. [Setup a Firestore client](https://firebase.google.com/docs/firestore/quickstart#set_up_your_development_environment)
2. Create a firestorm client and supply the names of the id and parent fields of your model structs.
Parent is optional. The id field must be a string but can be called anything.
```go
...
client, _ := app.Firestore(ctx)
fsc := firestorm.New(client, "ID", "")
```
3. Optional. For optimal caching to work consider adding the [CacheHandler](#cache).


#### Basic CRUD example

**Note:** Recursive Create/Delete is not supported and must be called on every entity. So to create an A->B relation. Create B first so the B.ID has been created and then create A.

```go
type Car struct {
	ID         string
	Make       string
	Year       time.Time
}
```
```go
car := &Car{}
car.Make = "Toyota"
car.Year, _ = time.Parse(time.RFC3339, "2001-01-01T00:00:00.000Z")

// Create the entity
fsc.NewRequest().CreateEntities(ctx, car)()

if car.ID == "" {
    t.Errorf("car should have an auto generated ID")
}

// Read the entity by ID
otherCar := &Car{ID:car.ID}
fsc.NewRequest().GetEntities(ctx, otherCar)()
if otherCar.Make != "Toyota" {
    t.Errorf("car should have name: Toyota but was: %s", otherCar.Make)
}
if otherCar.Year != car.Year {
    t.Errorf("car should have same year: %s", otherCar.Year)
}

// Update the entity
car.Make = "Jeep"
fsc.NewRequest().UpdateEntities(ctx, car)()

otherCar := &Car{ID:car.ID}
fsc.NewRequest().GetEntities(ctx, otherCar)()
if otherCar.Make != "Jeep" {
    t.Errorf("car should have name: Jeep but was: %s", otherCar.Make)
}

// Delete the entity
fsc.NewRequest().DeleteEntities(ctx, car)()

otherCar = &Car{ID:car.ID}
if err := fsc.NewRequest().GetEntities(ctx, otherCar)(); err == nil {
    t.Errorf("We expect a NotFoundError")
}
```
[More examples](https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go)

#### Search
Create a query using the firebase client

```go
car := &Car{}
car.ID = "testID"
car.Make = "Toyota"

fsc.NewRequest().CreateEntities(ctx, car)()

query := fsc.Client.Collection("Car").Where("make", "==", "Toyota")

result := make([]Car, 0)
if err := fsc.NewRequest().QueryEntities(ctx, query, &result)(); err != nil {
    t.Errorf("car was not found by search: %v", car)
}

if result[0].ID != car.ID || result[0].Make != car.Make {
    t.Errorf("entity did not match original entity : %v", result)
}
```
[More examples](https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go)

#### Concurrent requests
All CRUD operations are asynchronous and return a future func that when called will block until the operation is done.

**NOTE:** the state of the entities is undefined until the future func returns.
```go
car := &Car{Make:"Toyota"}

// Create the entity which returns a future func
future := fsc.NewRequest().CreateEntities(ctx, car)

// ID is not set
if car.ID != "" {
	t.Errorf("car ID should not have been set yet")
}

// do some more work

// blocks and waits for the database to finish
future()

// now the car has been saved and the ID has been set
if car.ID == "" {
    t.Errorf("car should have an auto generated ID now")
}
```
[More examples](https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go)

#### Transactions
Transactions are simply done in a function using the transaction context

```go
car := &Car{Make: "Toyota"}

fsc.DoInTransaction(ctx, func(transCtx context.Context) error {

    // Create the entity in the transaction using the transCtx
    fsc.NewRequest().CreateEntities(transCtx, car)()

    // Using the transCtx we can load the entity as it is saved in the session context
    otherCar := &Car{ID:car.ID}
    fsc.NewRequest().GetEntities(transCtx, otherCar)()
    if otherCar.Make != car.Make {
        t.Errorf("The car should have been saved in the transaction context")
    }

    // Loading using an other context (request) will fail as the car is not created until the func returns successfully
    if err := fsc.NewRequest().GetEntities(ctx, &Car{ID:car.ID})(); err == nil {
        t.Errorf("We expect a NotFoundError")
    }
})

// Now we can load the car as the transaction has been committed
otherCar := &Car{ID:car.ID}
fsc.NewRequest().GetEntities(ctx, otherCar)()
if otherCar.Make != "Toyota" {
    t.Errorf("car should have name: Toyota but was: %s", otherCar.Make)
}

```

[More examples](https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go)

#### Cache
Firestorm supports adding a session cache to the context.
The session cache only caches entities that are loaded within the same request.
```go
# add it to a single handler:
http.HandleFunc("/", firestorm.CacheHandler(otherHandler))
# or add it to the routing chain (for gorilla/mux, go-chi etc.):
r.Use(firestorm.CacheMiddleware)
```

To add a second level cache (such as Redis or memcache) the Cache interface needs to be implemented and added to the client:
```go
fsc.SetCache(c)
```

Firestore will first try to fetch an entity from the session cache. If it is not found it will try the second level cache.

#### Configurable auto load of references

Use the ```req.SetLoadPaths("fieldName")``` to auto load a particular field or ```req.SetLoadPaths(firestorm.AllEntities)``` to load all fields.

Load an entity path by adding multiple paths eg.: path->to->field

```go
fsc.NewRequest().SetLoadPaths("path", "path.to", "path.to.field").GetEntities(ctx, car)()
```
[More examples](https://github.com/jschoedt/go-firestorm/blob/master/tests/integration_test.go)

#### Help
Help is provided in the [go-firestorm User Group](https://groups.google.com/forum/?fromgroups#!forum/go-firestorm)
