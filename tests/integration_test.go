package firestormtests

import (
	"cloud.google.com/go/firestore"
	"context"
	"github.com/jschoedt/go-firestorm"
	"testing"
	"time"
)

type Person struct {
	ID     string
	Name   string
	Spouse *Person
}

type Car struct {
	ID         string
	Make       string
	Owner      *Person  // becomes firestore ref
	Driver     Person   // becomes a nested entity since he is not a reference
	Passengers []Person // becomes a firestore array of refs
	Tags       []string
	Numbers    []int
	Year       time.Time
}

func TestCRUD(t *testing.T) {
	car := &Car{}
	car.Make = "Toyota"
	car.Year, _ = time.Parse(time.RFC3339, "2001-01-01T00:00:00.000Z")

	// Create the entity
	fsc.NewRequest().CreateEntities(ctx, car)()

	if car.ID == "" {
		t.Errorf("car should have an auto generated ID")
	}

	// Read the entity by ID
	otherCar := &Car{ID: car.ID}
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

	otherCar = &Car{ID: car.ID}
	fsc.NewRequest().GetEntities(ctx, otherCar)()
	if otherCar.Make != "Jeep" {
		t.Errorf("car should have name: Jeep but was: %s", otherCar.Make)
	}

	// Delete the entity
	fsc.NewRequest().DeleteEntities(ctx, car)()

	otherCar = &Car{ID: car.ID}
	if _, err := fsc.NewRequest().GetEntities(ctx, otherCar)(); err == nil {
		t.Errorf("We expect a notFoundError")
	}
}

func TestSearch(t *testing.T) {
	car := &Car{}
	car.ID = "testID"
	car.Make = "Toyota"

	fsc.NewRequest().CreateEntities(ctx, car)()
	defer cleanup(car)

	query := fsc.Client.Collection("Car").Where("make", "==", "Toyota")

	result := make([]Car, 0)
	if err := fsc.NewRequest().QueryEntities(ctx, query, &result)(); err != nil {
		t.Errorf("car was not found by search: %v", car)
	}

	if result[0].ID != car.ID || result[0].Make != car.Make {
		t.Errorf("entity did not match original entity : %v", result)
	}

	ptrResult := make([]*Car, 0)
	if err := fsc.NewRequest().QueryEntities(ctx, query, &ptrResult)(); err != nil {
		t.Errorf("car was not found by search: %v", car)
	}

	if ptrResult[0].ID != car.ID || ptrResult[0].Make != car.Make {
		t.Errorf("entity did not match original entity : %v", ptrResult)
	}
}

func TestConcurrency(t *testing.T) {
	if testing.Short() {
		return
	}
	car := &Car{Make: "Toyota"}

	// Create the entity
	future := fsc.NewRequest().CreateEntities(ctx, car)
	defer cleanup(car)
	if car.ID != "" {
		t.Errorf("car ID should not have been set yet")
	}

	// so some more work

	// blocks and waits for the database to finish
	future()

	// now the car has been saved and the ID has been set
	if car.ID == "" {
		t.Errorf("car should have an auto generated ID now")
	}
}

func TestRelations(t *testing.T) {
	john := &Person{ID: "JohnsID", Name: "John"} // predefined ID
	mary := &Person{ID: "MarysID", Name: "Mary"}
	john.Spouse = mary
	mary.Spouse = john

	// Creates both values and references
	fsc.NewRequest().CreateEntities(ctx, []interface{}{john, mary})()
	defer cleanup(john, mary)

	// Reverting to the Firestore API we can test that the ref has been created
	snapshot, _ := fsc.Client.Collection("Person").Doc(john.ID).Get(ctx)
	if spouseRef, ok := snapshot.Data()["spouse"].(*firestore.DocumentRef); !ok {
		t.Errorf("spouse ref should have been a firestore.DocumentRef: %v", spouseRef)
	} else {
		if spouseRef.ID != mary.ID {
			t.Errorf("the id of the spouse ref should have been MarysID: %s", spouseRef.ID)
		}
	}
}

func TestAutoLoad(t *testing.T) {
	john := Person{ID: "JohnsID", Name: "John"} // predefined ID
	mary := Person{ID: "MarysID", Name: "Mary"}
	john.Spouse = &mary
	mary.Spouse = &john
	car := &Car{
		Make:       "Toyota",
		Owner:      &john,
		Driver:     Person{Name: "Mark"}, // embedded entity
		Passengers: []Person{john, mary},
		Tags:       []string{"tag1", "tag2"},
		Numbers:    []int{1, 2, 3},
	}
	car.Year, _ = time.Parse(time.RFC3339, "2001-01-01T00:00:00.000Z")

	// Creates both values and references
	fsc.NewRequest().CreateEntities(ctx, []interface{}{john, mary, car})()
	defer cleanup(john, mary, car)

	// Read the entity by ID
	otherCar := &Car{ID: car.ID}
	fsc.NewRequest().GetEntities(ctx, otherCar)()
	if otherCar.Make != "Toyota" && otherCar.Driver.Name == "Mark" &&
		len(otherCar.Tags) == 2 && len(otherCar.Numbers) == 3 {
		t.Errorf("saved element did not match original: %s", otherCar.Make)
	}

	// Read the car and its owner in one go. Note passengers are not loaded
	otherCar = &Car{ID: car.ID}
	fsc.NewRequest().SetLoadPaths("owner").GetEntities(ctx, otherCar)()
	if otherCar.Owner.ID != john.ID && len(otherCar.Passengers) == 0 {
		t.Errorf("The owners are the same so the IDs should be equal: %s", otherCar.Owner.ID)
	}

	// Read all references on the car
	otherCar = &Car{ID: car.ID}
	fsc.NewRequest().SetLoadPaths(firestorm.AllEntities).GetEntities(ctx, otherCar)()
	if otherCar.Owner.ID != john.ID || len(otherCar.Passengers) != 2 || otherCar.Passengers[0].ID != john.ID {
		t.Errorf("The owner and passengers should have been loaded: %v", otherCar)
	}

	// Also read the Spouses
	otherCar = &Car{ID: car.ID}
	fsc.NewRequest().SetLoadPaths(firestorm.AllEntities, "passengers.spouse").GetEntities(ctx, otherCar)()
	if otherCar.Passengers[0].Spouse == nil || otherCar.Passengers[0].Spouse.ID != mary.ID ||
		otherCar.Passengers[1].Spouse == nil || otherCar.Passengers[1].Spouse.ID != john.ID {
		t.Errorf("The owner and passengers should have been loaded: %v", otherCar)
	}

	// Since John's spouse was resolved as being a passenger it is also resolved as the owner
	if otherCar.Owner.Spouse.ID != mary.ID {
		t.Errorf("The owner and passengers should have been loaded: %v", otherCar)
	}
}

func TestCycles(t *testing.T) {
	john := &Person{ID: "JohnsID", Name: "John"} // predefined ID
	mary := &Person{ID: "MarysID", Name: "Mary"}
	john.Spouse = mary
	mary.Spouse = john

	// Creates both values and references
	fsc.NewRequest().CreateEntities(ctx, []interface{}{john, mary})()
	defer cleanup(john, mary)

	// Using auto load that is much simpler. Load John and spouse in one go
	john = &Person{ID: john.ID}
	fsc.NewRequest().SetLoadPaths("spouse").GetEntities(ctx, john)()
	if john.Spouse == nil || john.Spouse.ID != mary.ID {
		t.Errorf("Johns spouse should have been loaded: %v", john.Spouse)
	}

	// Also the back reference has been resolved to john
	john = &Person{ID: john.ID}
	fsc.NewRequest().SetLoadPaths("spouse", "spouse.spouse").GetEntities(ctx, john)()
	if john.Spouse.Spouse.ID != john.ID {
		t.Errorf("Johns spouse's spouse should be John: %v", john.Spouse.Spouse)
	}

	// Same result but only one round-trip to the database
	john = &Person{ID: john.ID}
	mary = &Person{ID: mary.ID}
	fsc.NewRequest().SetLoadPaths("spouse").GetEntities(ctx, []interface{}{john, mary})()
	if john.Spouse.Spouse.ID != john.ID {
		t.Errorf("Johns spouse's spouse should be John: %v", john.Spouse.Spouse)
	}
}

func TestTransactions(t *testing.T) {
	car := &Car{Make: "Toyota"}

	fsc.DoInTransaction(ctx, func(transCtx context.Context) error {

		// Create the entity in the transaction using the transCtx
		fsc.NewRequest().CreateEntities(transCtx, car)()

		// Using the transCtx we can load the entity as it is saved in the session context
		otherCar := &Car{ID: car.ID}
		fsc.NewRequest().GetEntities(transCtx, otherCar)()
		if otherCar.Make != car.Make {
			t.Errorf("The car should have been saved in the transaction context")
		}

		// Loading using an other context (request) will fail as the car is not created until the func returns successfully
		if _, err := fsc.NewRequest().GetEntities(ctx, &Car{ID: car.ID})(); err == nil {
			t.Errorf("We expect a notFoundError")
		}

		return nil
	})

	defer cleanup(car)

	// Now we can load the car as the transaction has been committed
	otherCar := &Car{ID: car.ID}
	fsc.NewRequest().GetEntities(ctx, otherCar)()
	if otherCar.Make != "Toyota" {
		t.Errorf("car should have name: Toyota but was: %s", otherCar.Make)
	}
}

type Moao struct {
	ID             string
	unexportedName string
}
type SubMoao struct {
	Moao
	LocalName string
}

func TestAnonymousStructs(t *testing.T) {
	sub := &SubMoao{}
	sub.unexportedName = "moao"
	sub.LocalName = "sub"

	// Create the entity
	fsc.NewRequest().CreateEntities(ctx, sub)()
	defer cleanup(sub)

	if sub.ID == "" {
		t.Errorf("element should have an auto generated ID")
	}

	// Read the entity by ID
	otherSub := &SubMoao{}
	otherSub.ID = sub.ID
	fsc.NewRequest().GetEntities(ctx, otherSub)()
	if otherSub.unexportedName != sub.unexportedName {
		t.Errorf("name should match: %s", otherSub.unexportedName)
	}
	if otherSub.LocalName != sub.LocalName {
		t.Errorf("name should match: %s", otherSub.LocalName)
	}
}

func cleanup(entities ...interface{}) {
	fsc.NewRequest().DeleteEntities(ctx, entities)()
}
