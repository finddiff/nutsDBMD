package bptree

type Manager struct {
	BPTreeIdx map[string]*Tree
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	panic("implement me")
}

func NewManager() *Manager {
	return &Manager{
		BPTreeIdx: map[string]*Tree{},
	}
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	if sl, ok := m.BPTreeIdx[bucket]; ok {
		sl.InsertOrUpdate(key, value)
	} else {
		sl = NewTree()
		m.BPTreeIdx[bucket] = sl
		sl.InsertOrUpdate(key, value)
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
	panic("implement me")
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
	panic("implement me")
}

func (m *Manager) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) DeleteBucket(bucket string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) MatchForRange(pattern, key string, f func(key string) bool) (end bool, err error) {
	//TODO implement me
	panic("implement me")
}
