package main

import (
	"testing"
)

func TestCreateDB(t *testing.T) {
	db := NewDB("test.db")
	defer db.Close()
	if db == nil {
		t.Error("Could not create db")
	}
}

func TestPersist(t *testing.T) {
	db := NewDB("test.db")
	defer db.Close()
	err := db.Persist("1234", map[string]interface{}{"a": 3, "b": 4})
	if err != nil {
		t.Error("Error persisting for node 1234", err)
	}
}

func TestPersistGet(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"a": 3, "b": 4}
	err := db.Persist("1234", vals)
	if err != nil {
		t.Error("Error persisting for node 1234", err)
	}
	res, err := db.GetPersist("1234", []string{"a", "b"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Error("Did not get key a for node 1234")
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestPersistOverwrite(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"a": 3, "b": 4}
	err := db.Persist("1234", vals)
	if err != nil {
		t.Error("Error persisting for node 1234", err)
	}
	res, err := db.GetPersist("1234", []string{"a", "b"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Error("Did not get key a for node 1234")
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
	vals = map[string]interface{}{"a": 5, "b": 4, "c": 7}
	err = db.Persist("1234", vals)
	if err != nil {
		t.Error("Error persisting for node 1234", err)
	}
	res, err = db.GetPersist("1234", []string{"a", "b", "c"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Error("Did not get key a for node 1234")
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestGetPersistSubset(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"a": 3, "b": 4}
	err := db.Persist("1234", vals)
	if err != nil {
		t.Error("Error persisting for node 1234", err)
	}
	res, err := db.GetPersist("1234", []string{"a"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range map[string]interface{}{"a": 3} {
		val, found = res[k]
		if !found {
			t.Error("Did not get key a for node 1234")
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestInsertGlobal(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"a": 1, "b": 2, "c": "hello"}
	err := db.Insert(vals)
	res, err := db.Get([]string{"a", "b", "c"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Error("Did not get key %v for global collection", k)
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestInsertCollection(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"col.a": 1, "col.b": 2, "col.c": "hello"}
	err := db.Insert(vals)
	if err != nil {
		t.Error("Could not insert", err)
	}
	res, err := db.Get([]string{"col.a", "col.b", "col.c"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Errorf("Did not get key %v for 'col' collection", k)
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestInsertTwoCollection(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"col.a": 1, "col.b": 2, "col2.c": "hello"}
	err := db.Insert(vals)
	if err != nil {
		t.Error("Could not insert", err)
	}
	res, err := db.Get([]string{"col.a", "col.b", "col2.c"})
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range vals {
		val, found = res[k]
		if !found {
			t.Errorf("Did not get key %v for 'col' collection", k)
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}

func TestGetBucket(t *testing.T) {
	var (
		val   interface{}
		found bool
	)
	db := NewDB("test.db")
	defer db.Close()
	vals := map[string]interface{}{"col.a": 1, "col.b": 2, "col2.c": "hello"}
	err := db.Insert(vals)
	if err != nil {
		t.Error("Could not insert", err)
	}
	res, err := db.GetBucket("col")
	if err != nil {
		t.Error("Could not get persist", err)
	}
	for k, v := range map[string]interface{}{"col.a": 1, "col.b": 2} {
		val, found = res[k]
		if !found {
			t.Errorf("Did not get key %v for 'col' collection", k)
		}
		if val != v {
			t.Errorf("Fetched value %v did not match %v", val, v)
		}
	}
}
