package main

import (
	"fmt"

	"github.com/bankole7782/files109"
)

func main() {
	rootFolder, ptName := "/tmp", "test1"
	err := files109.CreatePartition(rootFolder, ptName, 1, 10)
	if err != nil {
		panic(err)
	}

	err = files109.WriteFile(rootFolder, ptName, "jas.txt", []byte("Ja Ojo.\n"))
	if err != nil {
		panic(err)
	}

	err = files109.WriteFile(rootFolder, ptName, "hope.txt", []byte("Hoper Hope.\n"))
	if err != nil {
		panic(err)
	}

	files := []string{"james.txt", "jas.txt", "hope.txt"}
	for _, f := range files {
		data, err := files109.ReadFile(rootFolder, ptName, f)
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println(string(data))
	}

	elems, err := files109.ReadIndexPartition(rootFolder, ptName)
	if err != nil {
		panic(err)
	}

	fmt.Println(elems)
}
