package skiplist

type Manager struct {
	BPTreeIdx map[string]*SkipList
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	panic("implement me")
}

func NewManager() *Manager {
	return &Manager{
		BPTreeIdx: map[string]*SkipList{},
	}
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	//TODO implement me
	//panic("implement me")
	if sl, ok := m.BPTreeIdx[bucket]; ok {
		sl.Set(key, value)
	} else {
		sl = New(Bytes)
		m.BPTreeIdx[bucket] = sl
		sl.Set(key, value)
	}

	return nil
}

func (m *Manager) Get(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) GetAll(bucket string) ([]interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Manager) Find(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	panic("implement me")
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
