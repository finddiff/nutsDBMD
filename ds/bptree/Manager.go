package bptree

import (
	"bytes"
	"errors"
	"github.com/finddiff/nutsDBMD/ds/Iterator"
	"regexp"
)

type Manager struct {
	BPTreeIdx map[string]*Tree
}

func (m *Manager) Iterator(bucket string, startKey []byte, fn Iterator.ItemIterator) error {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		c := tree.findLeaf(startKey, false)
		starIndex := 0
		if c != nil {
			for starIndex = 0; starIndex < c.NumKeys; starIndex++ {
				if bytes.Compare(c.Keys[starIndex], startKey) > -1 {
					break
				}
			}
		}
		//call fn
		for c != nil {
			for i := starIndex; i < c.NumKeys; i++ {
				if !fn(c.Keys[i], c.Pointers[i]) {
					return nil
				}
			}
			starIndex = 0
			c, _ = c.Pointers[order-1].(*Node)
		}
	}
	return nil
}

func (m *Manager) FindAllBuckets() ([]string, error) {
	//TODO implement me
	buckets := []string{}
	for bucket, _ := range m.BPTreeIdx {
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (m *Manager) FindStart(bucket string) (interface{}, error) {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		tree.FindStart()
	}
	return nil, nil
}

func (m *Manager) Delete(bucket string, key []byte) error {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		err := tree.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		start := tree.findLeaf(prefix, false)
		if start == nil {
			start = tree.Root
		}
		if start == nil {
			return nil, 0, errors.New("key not found")
		}

		//find leaf date all save in leaf node
		for !start.IsLeaf {
			start = start.Pointers[0].(*Node)
		}

		rgx, err := regexp.Compile(reg)
		if err != nil {
			return nil, 0, errors.New("bad Regexp")
		}

		matchCount := 0
		prefixLen := len(prefix)
		isOverPrefix := false
		matchList := make([]interface{}, 0)
		for start != nil {

			for i := 0; i < start.NumKeys; i++ {
				if !isOverPrefix && bytes.Compare(prefix, start.Keys[i]) < 1 {
					isOverPrefix = true
				}
				if isOverPrefix {
					if len(start.Keys[i]) < prefixLen {
						return matchList, matchCount, nil
					}
					if bytes.Equal(prefix, start.Keys[i][:prefixLen]) {
						if !rgx.Match(bytes.TrimPrefix(start.Keys[i], prefix)) {
							continue
						}
						if matchCount > offsetNum {
							matchList = append(matchList, start.Pointers[i])
						}
						matchCount++
						if matchCount > offsetNum+limitNum {
							return matchList, matchCount, nil
						}
					} else {
						return matchList, matchCount, nil
					}
				}
			}

			start, _ = start.Pointers[order-1].(*Node)
		}
		return matchList, matchCount, nil
	}

	return nil, 0, nil
}

func NewManager() *Manager {
	return &Manager{
		BPTreeIdx: map[string]*Tree{},
	}
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	//var sl *Tree
	if sl, ok := m.BPTreeIdx[bucket]; !ok {
		sl = NewTree()
		m.BPTreeIdx[bucket] = sl
		err := sl.InsertOrUpdate(key, value)
		if err != nil {
			return err
		}
	} else {
		err := sl.InsertOrUpdate(key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) Get(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		item, err := tree.Find(key, false)
		if err != nil {
			return nil, err
		}
		return item, nil
	}

	return nil, nil
}

func (m *Manager) GetAll(bucket string) ([]interface{}, error) {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		return tree.All()
	}
	return nil, nil
}

func (m *Manager) Find(bucket string, key []byte) (interface{}, error) {
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		item, err := tree.Find(key, false)
		if err != nil {
			return nil, err
		}
		return item, nil
	}

	return nil, nil
}

func (m *Manager) RangeScan(bucket string, start, end []byte) ([]interface{}, error) {
	//TODO implement me
	//panic("implement me")
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		return tree.Range(start, end)
	}
	return nil, nil
}

func (m *Manager) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if tree, ok := m.BPTreeIdx[bucket]; ok {
		return tree.PrefixScan(prefix, offsetNum, limitNum)
	}
	return nil, 0, nil
}

func (m *Manager) DeleteBucket(bucket string) error {
	//TODO implement me
	if _, ok := m.BPTreeIdx[bucket]; ok {
		delete(m.BPTreeIdx, bucket)
	}
	return nil
}
