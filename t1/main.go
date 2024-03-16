package main

import "github.com/bankole7782/files109"

func main() {
	rootFolder, ptName := "/tmp", "test1"
	// err := files109.CreatePartition(rootFolder, ptName, 1, 10)
	// if err != nil {
	// 	panic(err)
	// }

	err := files109.WriteFile(rootFolder, ptName, "james.txt", []byte("James Ojo.\n"))
	if err != nil {
		panic(err)
	}
}
