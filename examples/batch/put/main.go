package main

import (
	"fmt"
	"log"

	nutsdb "github.com/finddiff/nutsDBMD"
	"github.com/xujiajun/utils/strconv2"
	"github.com/xujiajun/utils/time2"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("/tmp/nutsdbexample/example_batch"),
		// nutsdb.WithRWMode(nutsdb.MMap),
		// nutsdb.WithSyncEnable(false),
	)
	bucket = "bucket1"
}

func main() {
	time2.Start()

	end := 1

	for j := 1; j <= end; j++ {
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				for i := (j - 1) * 10000; i < j*10000; i++ {
					key := []byte("namename" + strconv2.IntToStr(i))
					val := []byte("valvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
					if err := tx.Put(bucket, key, val, 0); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
			log.Fatal(err)
		}

	}

	fmt.Println("batch put data cost: ", time2.End())
}
