package skiplist

import (
	"bytes"
	"errors"
	"github.com/finddiff/nutsDBMD/ds/Iterator"
	"regexp"
)

type Manager struct {
	SkiplistIdx map[string]*SkipList
}

func (m *Manager) Iterator(bucket string, startKey []byte, fn Iterator.ItemIterator) error {
	return nil
}

func (m *Manager) FindAllBuckets() ([]string, error) {
	//TODO implement me
	buckets := []string{}
	for bucket, _ := range m.SkiplistIdx {
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (m *Manager) FindStart(bucket string) (interface{}, error) {
	//TODO implement me
	return nil, nil
}

func (m *Manager) Delete(bucket string, key []byte) error {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {
		sl.Remove(key)
	}
	return nil
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {

		starter := sl.Find(prefix)
		if starter == nil {
			return nil, 0, errors.New("PrefixScan prefix no find")
		}

		rangeList := make([]interface{}, 0)
		count := 0
		el := starter
		prefixLen := len(prefix)
		matchReg, err := regexp.Compile(reg)
		if err != nil {
			return nil, 0, err
		}
		for el != nil {
			if !bytes.Equal(el.Key().([]byte)[:prefixLen], prefix) || !matchReg.Match(el.Key().([]byte)) {
				break
			}

			count++
			if count >= offsetNum {
				rangeList = append(rangeList, el)
			}

			if count >= offsetNum+limitNum {
				break
			}
			el = el.Next()
		}

		return rangeList, count, nil
	}
	return nil, 0, nil
}

func NewManager() *Manager {
	return &Manager{
		SkiplistIdx: map[string]*SkipList{},
	}
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {
		sl.Set(key, value)
	} else {
		sl = New(Bytes)
		m.SkiplistIdx[bucket] = sl
		sl.Set(key, value)
	}

	return nil
}

func (m *Manager) Get(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {
		value := sl.Get(key)
		return value, nil
	}

	return nil, nil
}

func (m *Manager) GetAll(bucket string) ([]interface{}, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {
		allList := make([]interface{}, 0)
		el := sl.Front()
		for el != nil {
			allList = append(allList, el)
			el = el.Next()
		}
	}

	return nil, nil
}

func (m *Manager) Find(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {
		value := sl.Get(key)
		return value, nil
	}

	return nil, nil
}

func (m *Manager) RangeScan(bucket string, start, end []byte) ([]interface{}, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {

		startEL := sl.Find(start)
		if startEL == nil {
			return nil, errors.New("RangeScan start no find")
		}

		endel := sl.Find(end)
		if endel == nil {
			return nil, errors.New("RangeScan start no find")
		}

		rangeList := make([]interface{}, 0)
		el := startEL
		for el != nil || el != endel {
			rangeList = append(rangeList, el)
			el = el.Next()
		}

		return rangeList, nil
	}

	return nil, nil
}

func (m *Manager) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	if sl, ok := m.SkiplistIdx[bucket]; ok {

		startEL := sl.Find(prefix)
		if startEL == nil {
			return nil, 0, errors.New("PrefixScan prefix no find")
		}

		rangeList := make([]interface{}, 0)
		count := 0
		el := startEL
		prefixLen := len(prefix)
		for el != nil {
			if !bytes.Equal(el.Key().([]byte)[:prefixLen], prefix) {
				break
			}

			count++
			if count >= offsetNum {
				rangeList = append(rangeList, el)
			}

			if count >= offsetNum+limitNum {
				break
			}
			el = el.Next()
		}

		return rangeList, count, nil
	}
	return nil, 0, nil
}

func (m *Manager) DeleteBucket(bucket string) error {
	//TODO implement me
	if _, ok := m.SkiplistIdx[bucket]; ok {
		delete(m.SkiplistIdx, bucket)
	}
	return nil
}
