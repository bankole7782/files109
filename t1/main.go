package main

import (
	"github.com/bankole7782/files109"
)

func main() {
	err := files109.CreatePartition("/tmp", "test1", 1, 10)
	if err != nil {
		panic(err)
	}
}
