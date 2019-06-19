package mapper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

type Moao struct {
	ID         string
	CreateTime time.Time
}

type CoreProject struct {
	Moao
	Name    string
	Owner   *CoreUser
	Members []CoreUser
	Writers []*CoreUser
}

type CoreUser struct {
	Moao
	Name     string
	MemberOf []*CoreProject
}

type DAOProject struct {
	createTimeField string
	idField         string
	Members         []DAOUser
	DAOProjectAllOf
}

type DAOUser struct {
	createTimeField string
	idField         string
	DAOProjectAllOf
}

func (m *DAOProject) ID() string {
	return m.idField
}

func (m *DAOProject) SetID(id string) {
	m.idField = id
}

func (m *DAOProject) CreateTime() string {
	return m.createTimeField
}

func (m *DAOProject) SetCreateTime(time string) {
	m.createTimeField = time
}

type DAOProjectAllOf struct {
	Name string
}

func TestMapEntity2Model(t *testing.T) {
	ep := &CoreProject{}
	ep.ID = "projectId"
	ep.Name = "myName"
	ep.CreateTime = time.Now()

	user := CoreUser{}
	user.Name = "user1"
	ep.Members = append(ep.Members, user)
	ep.Writers = append(ep.Writers, &user)

	mp := &DAOProject{}

	mapperFunc := func(inKey string, inVal reflect.Value) (string, reflect.Value) {
		switch inKey {
		case "id":
			return "idfield", inVal
		}
		if strings.Contains(inKey, "time") {
			return inKey + "field", reflect.ValueOf(marshal(inVal.Interface()))
		}
		//return strings.TrimSuffix(in, "field")
		return inKey, inVal
	}

	mapper := NewWithFunc(mapperFunc)

	if err := mapper.MapTo(ep, mp); err != nil {
		t.Errorf("Error calling mapEntity %v", err)
	}

	fmt.Printf("mp: %v  ", mp)
	if ep.ID != mp.ID() {
		t.Errorf("Value not the same: %s, want: %s.", mp.ID(), ep.ID)
	}
	if ep.Name != mp.Name {
		t.Errorf("Value not the same: %s, want: %s.", mp.Name, ep.Name)
	}
	if marshal(ep.CreateTime) != mp.CreateTime() {
		t.Errorf("Value not the same: %s, want: %s.", mp.CreateTime(), ep.CreateTime.String())
	}
	if len(mp.Members) != 1 || mp.Members[0].Name != user.Name {
		t.Errorf("Value not the same: %s, want: %s.", mp.Members, user.Name)
	}
}

func TestMapModel2Entity(t *testing.T) {
	mp := &DAOProject{}
	mp.SetID("projectId")
	mp.Name = "myName"
	mp.SetCreateTime(marshal(time.Now()))

	ep := &CoreProject{}

	mapperFunc := func(inKey string, inVal reflect.Value) (string, reflect.Value) {
		inKey = strings.TrimSuffix(inKey, "field")
		if strings.Contains(inKey, "time") {
			t := time.Now()
			json.Unmarshal([]byte(inVal.Interface().(string)), &t)
			return inKey, reflect.ValueOf(t)
		}
		return inKey, inVal
	}

	mapper := NewWithFunc(mapperFunc)

	if err := mapper.MapTo(mp, ep); err != nil {
		t.Errorf("Error calling mapEntity %v", err)
	}

	fmt.Printf("mp: %v  ", mp)
	if ep.ID != mp.ID() {
		t.Errorf("Value not the same: %s, want: %s.", ep.ID, mp.ID())
	}
	if ep.Name != mp.Name {
		t.Errorf("Value not the same: %s, want: %s.", ep.Name, mp.Name)
	}
	if marshal(ep.CreateTime) != mp.CreateTime() {
		t.Errorf("Value not the same: %s, want: %s.", ep.CreateTime.String(), mp.CreateTime())
	}
}

func TestMapMap2NestedEntity(t *testing.T) {
	valMap := make(map[string]interface{})
	valMap["id"] = "projectId"
	valMap["name"] = "projectName"

	ownerMap := make(map[string]interface{})
	valMap["owner"] = ownerMap
	ownerMap["id"] = "ownerId"
	ownerMap["name"] = "ownerName"
	memberOf := []map[string]interface{}{valMap}
	ownerMap["memberof"] = memberOf

	members := []map[string]interface{}{ownerMap}
	valMap["members"] = members

	writers := []map[string]interface{}{ownerMap}
	valMap["writers"] = writers

	ep := &CoreProject{}
	mapper := New()

	if err := mapper.MapTo(valMap, ep); err != nil {
		t.Errorf("Error calling MapTo %v", err)
	}

	fmt.Printf("mp: %v  ", ep)
	if ep.ID != valMap["id"] {
		t.Errorf("Value not the same: %#v, want: %s.", ep.ID, valMap["id"])
	}
	if ep.Name != valMap["name"] {
		t.Errorf("Value not the same: %#v, want: %s.", ep.Name, valMap["name"])
	}

	if ep.Owner == nil || ep.Owner.Name != ownerMap["name"] {
		t.Errorf("Value not the same: %#v, want: %s.", ep.Owner, ownerMap["name"])
	}

	if len(ep.Owner.MemberOf) != 1 || ep.Owner.MemberOf[0].Name != valMap["name"] {
		t.Errorf("Value not the same: %s, want: %s.", ep.Owner.MemberOf, ownerMap["name"])
	}

	if len(ep.Members) != 1 || ep.Members[0].Name != ownerMap["name"] {
		t.Errorf("Value not the same: %#v, want: %s.", ep.Members, ownerMap["name"])
	}

	if len(ep.Writers) != 1 || ep.Writers[0].Name != ownerMap["name"] {
		t.Errorf("Value not the same: %s, want: %s.", ep.Writers, ownerMap["name"])
	}

}

func marshal(val interface{}) string {
	d, err := json.Marshal(val)
	if err != nil {
		return ""
	}
	return string(d)
}
