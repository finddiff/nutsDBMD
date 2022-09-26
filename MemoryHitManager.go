package nutsDBMD

import (
	"github.com/finddiff/nutsDBMD/ds/Iterator"
	"github.com/finddiff/nutsDBMD/ds/bptree"
	"github.com/finddiff/nutsDBMD/ds/skiplist"
)

type MemHit interface {
	Set(bucket string, key []byte, value interface{}) error
	Get(bucket string, key []byte) (interface{}, error)
	GetAll(bucket string) ([]interface{}, error)
	Find(bucket string, key []byte) (interface{}, error)
	FindStart(bucket string) (interface{}, error)
	FindAllBuckets() ([]string, error)
	Iterator(bucket string, startKey []byte, fn Iterator.ItemIterator) error
	RangeScan(bucket string, start, end []byte) ([]interface{}, error)
	PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) ([]interface{}, int, error)
	PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]interface{}, int, error)
	DeleteBucket(bucket string) error
	Delete(bucket string, key []byte) error
}

func (db *DB) initDataHitMemStruct() error {
	switch db.opt.HitMode {
	case Bptree:
		bptree.SetOrder(db.opt.Order)
		db.DataHitMemStruct = bptree.NewManager()
	//case Btree:
	//	db.DataHitMemStruct = btree.
	case Skiplist:
		db.DataHitMemStruct = skiplist.NewManager()
	default:
		bptree.SetOrder(db.opt.Order)
		db.DataHitMemStruct = bptree.NewManager()
	}
	return nil
}
