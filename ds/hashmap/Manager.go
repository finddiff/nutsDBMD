package hashmap

import "errors"

type Manager struct {
	MapIdx map[string]*hashmap
}

func (m *Manager) Set(bucket string, key []byte, value interface{}) error {
	//TODO implement me
	if dsmap, ok := m.MapIdx[bucket]; ok {
		dsmap.dsmap[string(key)] = value
	}
	return nil
}

func (m *Manager) Get(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	if dsmap, ok := m.MapIdx[bucket]; ok {
		if value, ok := dsmap.dsmap[string(key)]; ok {
			return value, nil
		}
	}
	return nil, nil
}

func (m *Manager) GetAll(bucket string) ([]interface{}, error) {
	//TODO implement me
	return nil, errors.New("implement me")
}

func (m *Manager) Find(bucket string, key []byte) (interface{}, error) {
	//TODO implement me
	return m.Get(bucket, key)
}

func (m *Manager) RangeScan(bucket string, start, end []byte) ([]interface{}, error) {
	//TODO implement me
	return nil, errors.New("implement me")
}

func (m *Manager) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	return nil, 0, errors.New("implement me")
}

func (m *Manager) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error) {
	//TODO implement me
	return nil, 0, errors.New("implement me")
}

func (m *Manager) DeleteBucket(bucket string) error {
	//TODO implement me
	if _, ok := m.MapIdx[bucket]; ok {
		delete(m.MapIdx, bucket)
	}
	return nil
}

func (m *Manager) Delete(bucket string, key []byte) error {
	//TODO implement me
	//return errors.New("implement me")
	if _, ok := m.MapIdx[bucket]; ok {
		if _, ok = m.MapIdx[bucket].dsmap[string(key)]; ok {
			delete(m.MapIdx[bucket].dsmap, string(key))
		}
	}

	return nil
}
