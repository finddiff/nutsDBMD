package critbit

import (
	"github.com/finddiff/nutsDBMD/ds/Iterator"
	"regexp"
)

type Manager struct {
	CritbitMap map[string]*Tree
}

func (m *Manager) Iterator(bucket string, startKey []byte, fn Iterator.ItemIterator) error {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		tree.Walk(fn)
	}
	return nil
}

func (m *Manager) FindAllBuckets() ([]string, error) {
	//TODO implement me
	buckets := []string{}
	for bucket, _ := range m.CritbitMap {
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (m *Manager) FindStart(bucket string) (interface{}, error) {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		if _, value, ok := tree.Minimum(); ok {
			return value, nil
		}
	}
	return nil, nil
}

func (m *Manager) Delete(bucket string, key []byte) error {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		if _, ok := tree.Delete(key); ok {
			return nil
		}
	}
	return nil
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		resultlist := make([]interface{}, 0)
		count := 0
		endcount := offsetNum + limitNum
		re := regexp.MustCompile(reg)
		tree.WalkPrefix(prefix, func(key []byte, value interface{}) bool {
			if count >= offsetNum && count > endcount && re.Match(key) {
				resultlist = append(resultlist, value)
			} else {
				return false
			}
			count++
			return true
		})
		return resultlist, count, nil
	}

	return nil, 0, nil
}

func NewManager() *Manager {
	return &Manager{
		CritbitMap: map[string]*Tree{},
	}
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	//var sl *Tree
	if _, ok := m.CritbitMap[bucket]; !ok {
		m.CritbitMap[bucket] = New()
	}
	if _, ok := m.CritbitMap[bucket].Insert(key, value); ok {
		return nil
	}
	return nil
}

func (m *Manager) Get(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		if value, ok := tree.Get(key); ok {
			return value, nil
		}
		return nil, nil
	}

	return nil, nil
}

func (m *Manager) GetAll(bucket string) ([]interface{}, error) {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		resultlist := make([]interface{}, 0)
		tree.Walk(func(key []byte, value interface{}) bool {
			resultlist = append(resultlist, value)
			return true
		})
		return resultlist, nil
	}
	return nil, nil
}

func (m *Manager) Find(bucket string, key []byte) (interface{}, error) {
	return m.Get(bucket, key)
}

func (m *Manager) RangeScan(bucket string, start, end []byte) ([]interface{}, error) {
	//TODO implement me
	//panic("implement me")
	//if tree, ok := m.CritbitMap[bucket]; ok {
	//	return tree.Range(start, end)
	//}
	return nil, nil
}

func (m *Manager) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if tree, ok := m.CritbitMap[bucket]; ok {
		resultlist := make([]interface{}, 0)
		count := 0
		endcount := offsetNum + limitNum
		tree.WalkPrefix(prefix, func(key []byte, value interface{}) bool {
			if count >= offsetNum && count > endcount {
				resultlist = append(resultlist, value)
			} else {
				return false
			}
			count++
			return true
		})
		return resultlist, count, nil
	}
	return nil, 0, nil
}

func (m *Manager) DeleteBucket(bucket string) error {
	//TODO implement me
	if _, ok := m.CritbitMap[bucket]; ok {
		delete(m.CritbitMap, bucket)
	}
	return nil
}
