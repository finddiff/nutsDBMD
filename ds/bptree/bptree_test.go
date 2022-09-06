package bptree

import (
	"fmt"
	"reflect"
	"testing"
)

func hello() {
	fmt.Println("bptree says 'hello friend'")
}

func TestInsertNilRoot(t *testing.T) {
	tree := NewTree()
	hello()

	key := []byte("test-key1")
	value := []byte("test-value")

	err := tree.Insert(key, value)

	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r)
	}
}

func TestInsert(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}

	err = tree.Insert(key, append(value, []byte("world1")...))
	if err == nil {
		t.Errorf("expected error but got nil")
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	if tree.Root.NumKeys > 1 {
		t.Errorf("expected 1 key and got %d", tree.Root.NumKeys)
	}
}

func TestInsertSameValueTwice(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key2"), value)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	if tree.Root.NumKeys <= 1 {
		t.Errorf("expected more than 1 key and got %d", tree.Root.NumKeys)
	}
}

func TestFindNilRoot(t *testing.T) {
	tree := NewTree()

	r, err := tree.Find([]byte("test-key"), false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("expected nil got %v \n", r)
	}
}

func TestFind(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}
}

func TestDeleteNilTree(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")

	err := tree.Delete(key)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	r, err := tree.Find(key, false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete \n")
	}
}

func TestDelete(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	err = tree.Delete(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find(key, false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete \n")
	}
}

func TestDeleteNotFound(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	err = tree.Delete([]byte("test-key2"))
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	r, err = tree.Find([]byte("test-key2"), false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}
}

func TestMultiInsertSingleDelete(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key2"), append(value, []byte("world1")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key3"), append(value, []byte("world2")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key4"), append(value, []byte("world3")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key5"), append(value, []byte("world4")...))
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	err = tree.Delete(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find(key, false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete - %v \n", r)
	}
}

func TestMultiInsertMultiDelete(t *testing.T) {
	tree := NewTree()

	key := []byte("test-key1")
	value := []byte("test")

	err := tree.Insert(key, value)
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key2"), append(value, []byte("world1")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key3"), append(value, []byte("world2")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key4"), append(value, []byte("world3")...))
	if err != nil {
		t.Errorf("%s", err)
	}
	err = tree.Insert([]byte("test-key5"), append(value, []byte("world4")...))
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key, false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	err = tree.Delete(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find(key, false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete - %v \n", r)
	}

	r, err = tree.Find([]byte("test-key4"), false)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.(*RecordItem).Value, append(value, []byte("world3")...)) {
		t.Errorf("expected %v and got %v \n", value, r.(*RecordItem).Value)
	}

	err = tree.Delete([]byte("test-key4"))
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find([]byte("test-key4"), false)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete - %v \n", r)
	}
}
