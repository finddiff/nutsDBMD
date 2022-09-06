package main

import (
	"fmt"
	nutsdb "github.com/finddiff/nutsDBMD"
	"io/ioutil"
	"log"
	"os"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	fileDir := "/tmp/nutsdb_example"

	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			fmt.Println(fileDir + "/" + name)
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}

	fileDir = "D:\\user\\weiyc\\document\\GO\\src\\nutshttp\\examples\\nutsdb"

	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(fileDir),
		nutsdb.WithSegmentSize(1024*1024), // 1MB
	)
	db.Backup("/back")
	bucket = "bucketForString"
}

func main() {
	// insert
	put()
	// read
	read()

	// delete
	delete()
	// read
	read()

	// insert
	put()
	// read
	read()

	// update
	put2()
	// read
	read2()

	readall()
}

func readall() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			ens, err := tx.GetAll(bucket)
			for _, e := range ens {
				fmt.Println("GetAll val:", string(e.Value))
			}
			return err
			//return tx.Delete(bucket, key)
		}); err != nil {
		log.Fatal(err)
	}
}

func delete() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			return tx.Delete(bucket, key)
		}); err != nil {
		log.Fatal(err)
	}
}

func put() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val1")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}
func put2() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name2")
			val := []byte("val2")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}

func read() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			e, err := tx.Get(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("val:", string(e.Value))

			return nil
		}); err != nil {
		log.Println(err)
	}
}

func read2() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name2")
			e, err := tx.Get(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("val:", string(e.Value))

			return nil
		}); err != nil {
		log.Println(err)
	}
}
