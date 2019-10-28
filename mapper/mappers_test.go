package mapper

import (
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

type Id struct {
	ID string
}

type Person struct {
	Id
	Name      string
	Spouse    *Person
	Relations []*Relation // becomes nested as Relation is missing ID
}

type Car struct {
	Make       string
	Owner      *Person  // becomes firestore ref
	Driver     Person   // becomes a nested entity since he is not a reference
	Passengers []Person // becomes a firestore array of refs
	Tags       []string
	Numbers    []int
	Year       time.Time
}

type Relation struct {
	Name    string
	Friends []*Person // becomes a firestore array of refs
}

func TestStructToMap(t *testing.T) {
	john := Person{Name: "John"}
	mary := Person{Name: "Mary"}
	john.Spouse = &mary
	mary.Spouse = &john

	friend1 := &Person{Name: "Friend1"}
	friend2 := &Person{Name: "Friend2"}

	// Add the nested relation
	john.Relations = []*Relation{{Friends: []*Person{friend1, friend2}}}

	now := time.Now()

	car := &Car{
		Make:       "Toyota",
		Owner:      &john,
		Driver:     Person{Name: "Mark"}, // embedded entity
		Passengers: []Person{john, mary},
		Tags:       []string{"tag1", "tag2"},
		Numbers:    []int{1, 2, 3},
		Year:       now,
	}

	mapper := New()
	m, err := mapper.MapStructToMap(car)
	if err != nil {
		t.Errorf("Could not convert struct to map %v", err)
	}

	newCar := &Car{}
	if err := mapper.MapTo(m, newCar); err != nil {
		t.Errorf("Could not map to struct %v", err)
	}

	// test and break the cycle before comparing
	if car.Owner.Spouse.Name != newCar.Owner.Spouse.Name || car.Owner.Spouse.Spouse.Name != newCar.Owner.Spouse.Spouse.Name {
		t.Errorf("The structs cycle did not match %v - %v", car, newCar)
	}

	car.Owner.Spouse.Spouse = nil
	newCar.Owner.Spouse.Spouse = nil

	if !cmp.Equal(car, newCar) {
		t.Errorf("The structs were not the same %v - %v", car, newCar)
	}
}
