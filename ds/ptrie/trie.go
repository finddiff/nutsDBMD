package ptrie

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"reflect"
	"sync"
)

// Merger represents node value merger
type Merger func(previous, next interface{}) (merged interface{})

// OnMatch represents matching input handler, return value instruct trie to continue search
type OnMatch func(key []byte, value interface{}) bool

// Visitor represents value node visitor handler
type Visitor func(key []byte, value interface{}) bool

// Trie represents prefix tree interface
type Trie interface {
	Put(key []byte, value interface{}) error

	Merge(key []byte, value interface{}, merger Merger) error

	Get(key []byte) (interface{}, bool)

	Has(key []byte) bool

	//Walk all tries value nodes.
	Walk(handler Visitor)

	//MatchPrefix matches input prefix, ie. input: dev.domain.com, would match with trie keys like: dev, dev.domain
	MatchPrefix(input []byte, handler OnMatch) bool

	//MatchAll matches input with any occurrences of tries keys.
	MatchAll(input []byte, handler OnMatch) bool

	UseType(vType reflect.Type)

	//Decode decodes concurrently trie nodes and values
	Decode(reader io.Reader) error

	//DecodeSequentially decode sequentially trie nodes and values
	DecodeSequentially(reader io.Reader) error

	Encode(writer io.Writer) error

	ValueCount() int
}

type trie struct {
	values *values
	root   *Node
	bset   Bit64Set
}

func (t *trie) Put(key []byte, value interface{}) error {
	return t.Merge(key, value, nil)
}

func (t *trie) Merge(key []byte, value interface{}, merger Merger) error {
	return t.merge(t.root, key, value, merger)
}

func (t *trie) merge(root *Node, key []byte, value interface{}, merger Merger) error {
	t.bset = t.bset.Put(key[0])
	index, err := t.values.put(value)
	if err != nil {
		return err
	}
	node := newValueNode(key, index)
	root.add(node, func(prev uint32) uint32 {
		if merger != nil {
			prevValue := t.values.value(prev)
			newValue := merger(prevValue, value)
			index, e := t.values.put(newValue)
			if e != nil {
				err = e
			}
			return index
		}
		return node.ValueIndex
	})
	return err
}

func (t *trie) Get(key []byte) (interface{}, bool) {
	var result interface{}
	found := false
	handler := func(k []byte, value interface{}) bool {
		if found = len(key) == len(k); found {
			result = value
			return false
		}
		return true
	}
	has := t.root.match(key, 0, func(key []byte, valueIndex uint32) bool {
		value := t.values.value(valueIndex)
		return handler(key, value)
	})
	if !has {
		return nil, has
	}
	return result, found
}

func (t *trie) Has(key []byte) bool {
	_, has := t.Get(key)
	return has
}

func (t *trie) MatchPrefix(input []byte, handler OnMatch) bool {
	return t.match(t.root, input, handler)
}

func (t *trie) Walk(handler Visitor) {
	t.root.walk([]byte{}, func(key []byte, valueIndex uint32) {
		value := t.values.value(valueIndex)
		handler(key, value)
	})
}

func (t *trie) UseType(vType reflect.Type) {
	t.values.useType(vType)
}

func (t *trie) decodeValues(reader io.Reader, err *error, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	if e := t.values.Decode(reader); e != nil {
		*err = e
	}
}

func (t *trie) decodeTrie(root *Node, reader io.Reader, err *error, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	if e := root.Decode(reader); e != nil {
		*err = e
	}
}

func (t *trie) Decode(reader io.Reader) error {
	return t.decodeConcurrently(reader)
}

func (t *trie) decodeConcurrently(reader io.Reader) error {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(2)
	trieLength := uint64(0)
	bset := uint64(0)
	err := binary.Read(reader, binary.LittleEndian, &bset)
	if err == nil {
		t.bset = Bit64Set(bset)
		if err = binary.Read(reader, binary.LittleEndian, &trieLength); err != nil {
			return err
		}
	}
	data := make([]byte, trieLength)
	if err = binary.Read(reader, binary.LittleEndian, data); err != nil {
		return err
	}
	go t.decodeTrie(t.root, bytes.NewReader(data), &err, waitGroup)

	dataReader, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	go t.decodeValues(bytes.NewReader(dataReader), &err, waitGroup)
	waitGroup.Wait()
	return err
}

func (t *trie) DecodeSequentially(reader io.Reader) error {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(2)
	trieLength := uint64(0)
	bset := uint64(0)
	err := binary.Read(reader, binary.LittleEndian, &bset)
	if err == nil {
		t.bset = Bit64Set(bset)
		if err = binary.Read(reader, binary.LittleEndian, &trieLength); err != nil {
			return err
		}
	}
	t.decodeTrie(t.root, reader, &err, waitGroup)
	t.decodeValues(reader, &err, waitGroup)
	waitGroup.Wait()
	return err
}

func (t *trie) encodeTrie(root *Node, writer io.Writer) error {
	return root.Encode(writer)
}

func (t *trie) encodeValues(writer io.Writer) error {
	return t.values.Encode(writer)
}

func (t *trie) ValueCount() int {
	return len(t.values.data)
}

func (t *trie) Encode(writer io.Writer) error {
	trieSize := t.root.size()
	err := binary.Write(writer, binary.LittleEndian, uint64(t.bset))
	if err == nil {
		if err = binary.Write(writer, binary.LittleEndian, uint64(trieSize)); err == nil {
			if err = t.encodeTrie(t.root, writer); err == nil {
				err = t.encodeValues(writer)
			}
		}
	}
	return err
}

func (t *trie) MatchAll(input []byte, handler OnMatch) bool {
	toContinue := true
	matched := false
	for i := 0; i < len(input); i++ {

		if t.bset > 0 && !t.bset.IsSet(input[i]) {
			continue
		}
		if hasMatched := t.match(t.root, input[i:], func(key []byte, value interface{}) bool {
			toContinue = handler(key, value)
			return toContinue
		}); hasMatched {
			matched = true
		}
		if !toContinue {
			break
		}
	}
	return matched
}

func (t *trie) match(root *Node, input []byte, handler OnMatch) bool {
	return root.match(input, 0, func(key []byte, valueIndex uint32) bool {
		value := t.values.value(valueIndex)
		return handler(key, value)
	})
}

// New create new prefix trie
func New() Trie {
	return &trie{
		values: newValues(),
		root:   newValueNode([]byte{}, 0),
	}
}
