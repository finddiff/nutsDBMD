// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsDBMD

import (
	"fmt"
	"time"
)

func getNewKey(bucket string, key []byte) []byte {
	newKey := []byte(bucket)
	newKey = append(newKey, key...)
	return newKey
}

func (tx *Tx) getAllByHintBPTSparseIdx(bucket string) (entries Entries, err error) {
	bucketMeta, err := ReadBucketMeta(tx.db.getBucketMetaFilePath(bucket))
	if err != nil {
		return nil, err
	}

	return tx.RangeScan(bucket, bucketMeta.start, bucketMeta.end)
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	idxMode := tx.db.opt.EntryIdxMode

	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {
		//if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
		r, err := tx.db.DataHitMemStruct.Find(bucket, key)
		if err != nil {
			return nil, err
		}

		if _, ok := tx.db.committedTxIds[r.(*Record).H.Meta.TxID]; !ok {
			return nil, ErrNotFoundKey
		}

		if r.(*Record).H.Meta.Flag == DataDeleteFlag || r.(*Record).IsExpired() {
			return nil, ErrNotFoundKey
		}

		if idxMode == HintKeyValAndRAMIdxMode {
			return r.(*Record).E, nil
		}

		if idxMode == HintKeyAndRAMIdxMode {
			path := tx.db.getDataPath(r.(*Record).H.FileID)
			df, err := tx.db.fm.getDataFile(path, tx.db.opt.SegmentSize)
			if err != nil {
				return nil, err
			}
			defer func(rwManager RWManager) {
				err := rwManager.Release()
				if err != nil {
					return
				}
			}(df.rwManager)

			item, err := df.ReadAt(int(r.(*Record).H.DataPos))
			if err != nil {
				return nil, fmt.Errorf("read err. pos %d, key %s, err %s", r.(*Record).H.DataPos, string(key), err)
			}

			return item, nil
		}
		//}
	}

	return nil, ErrBucketAndKey(bucket, key)
}

// GetAll returns all keys and values of the bucket stored at given bucket.
func (tx *Tx) GetAll(bucket string) (entries Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = Entries{}

	idxMode := tx.db.opt.EntryIdxMode

	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {
		records, err := tx.db.DataHitMemStruct.GetAll(bucket)
		if err != nil {
			return nil, ErrBucketEmpty
		}

		entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries, "RangeScan")
		if err != nil {
			return nil, ErrBucketEmpty
		}
	}

	if len(entries) == 0 {
		return nil, ErrBucketEmpty
	}

	return
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (es Entries, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	records, err := tx.db.DataHitMemStruct.RangeScan(bucket, start, end)
	if err != nil {
		return nil, ErrRangeScan
	}

	es, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, es, "RangeScan")
	if err != nil {
		return nil, ErrRangeScan
	}

	if len(es) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (es Entries, off int, err error) {

	if err := tx.checkTxIsClosed(); err != nil {
		return nil, off, err
	}

	records, voff, err := tx.db.DataHitMemStruct.PrefixScan(bucket, prefix, offsetNum, limitNum)
	if err != nil {
		off = voff
		return nil, off, ErrPrefixScan
	}

	es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, "PrefixScan")
	if err != nil {
		off = voff
		return nil, off, ErrPrefixScan
	}

	off = voff

	if len(es) == 0 {
		return nil, off, ErrPrefixScan
	}

	return
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (es Entries, off int, err error) {

	if err := tx.checkTxIsClosed(); err != nil {
		return nil, off, err
	}

	records, voff, err := tx.db.DataHitMemStruct.PrefixSearchScan(bucket, prefix, reg, offsetNum, limitNum)
	if err != nil {
		off = voff
		return nil, off, ErrPrefixSearchScan
	}

	es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, "PrefixSearchScan")
	if err != nil {
		off = voff
		return nil, off, ErrPrefixSearchScan
	}

	off = voff

	if len(es) == 0 {
		return nil, off, ErrPrefixSearchScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records []interface{}, limitNum int, es Entries, scanMode string) (Entries, error) {
	for _, ir := range records {
		r := ir.(*Record)
		if r.H.Meta.Flag == DataDeleteFlag || r.IsExpired() {
			continue
		}

		if limitNum > 0 && len(es) < limitNum || limitNum == ScanNoLimit {
			idxMode := tx.db.opt.EntryIdxMode
			if idxMode == HintKeyAndRAMIdxMode {
				path := tx.db.getDataPath(r.H.FileID)
				df, err := tx.db.fm.getDataFile(path, tx.db.opt.SegmentSize)
				if err != nil {
					return nil, err
				}
				if item, err := df.ReadAt(int(r.H.DataPos)); err == nil {
					es = append(es, item)
				} else {
					releaseErr := df.rwManager.Release()
					if releaseErr != nil {
						return nil, releaseErr
					}
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.DataPos, err)
				}
				err = df.rwManager.Release()
				if err != nil {
					return nil, err
				}
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				es = append(es, r.E)
			}
		}
	}

	return es, nil
}
