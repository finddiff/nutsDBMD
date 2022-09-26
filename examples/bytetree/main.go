package main

import "github.com/finddiff/nutsDBMD/ds/bytetree"

func main() {
	t := bytetree.NewTree()

	t.InsertOrUpdate([]byte("b"), "just b")
	t.InsertOrUpdate([]byte("a"), "just a")
	t.InsertOrUpdate([]byte("bc"), "just bc")
	t.InsertOrUpdate([]byte("ab"), "just ab")
	t.InsertOrUpdate([]byte("bb"), "just bb")

	return
}
