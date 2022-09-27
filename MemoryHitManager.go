package nutsDBMD

import (
	"github.com/finddiff/nutsDBMD/ds/Iterator"
	"github.com/finddiff/nutsDBMD/ds/bptree"
	"github.com/finddiff/nutsDBMD/ds/critbit"
	"github.com/finddiff/nutsDBMD/ds/hashmap"
	"github.com/finddiff/nutsDBMD/ds/skiplist"
)

type MemHit interface {
	Set(bucket string, key []byte, value interface{}) error
	Get(bucket string, key []byte) (interface{}, error)
	GetAll(bucket string) ([]interface{}, error)
	Find(bucket string, key []byte) (interface{}, error)
	FindStart(bucket string) (interface{}, error)
	FindAllBuckets() ([]string, error)
	//for free invalidy key
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
	case Skiplist:
		db.DataHitMemStruct = skiplist.NewManager()
	//case Btree:
	//	db.DataHitMemStruct = btree.NewManager()
	case CritBit:
		db.DataHitMemStruct = critbit.NewManager()
	case HashMap:
		db.DataHitMemStruct = hashmap.NewManager()
	default:
		bptree.SetOrder(db.opt.Order)
		db.DataHitMemStruct = bptree.NewManager()
	}
	return nil
}
