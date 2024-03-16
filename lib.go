package files109

import (
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

func CreatePartition(linuxFolderPath, partitionName string, indexPartionSizeInGBs, dataPartitionSizeInGBs int) error {
	// validate partition name
	err := nameValidate(partitionName)
	if err != nil {
		return err
	}

	// make first partition
	indexPartitionPath := filepath.Join(linuxFolderPath, partitionName+".f109a")
	indexPartionSizeInBytes := indexPartionSizeInGBs * int(math.Pow10(9))

	err = makeLargeFileEmpty(indexPartitionPath, indexPartionSizeInBytes)
	if err != nil {
		return err
	}

	// make second partition
	dataPartitionPath := filepath.Join(linuxFolderPath, partitionName+".f109b")
	dataPartitionSizeInBytes := dataPartitionSizeInGBs * int(math.Pow10(9))

	err = makeLargeFileEmpty(dataPartitionPath, dataPartitionSizeInBytes)
	if err != nil {
		return err
	}

	return nil
}

func makeLargeFileEmpty(pathOfLargeFile string, sizeInBytes int) error {
	largeFileHandler, err := os.Create(pathOfLargeFile)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer largeFileHandler.Close()

	rawData := make([]byte, sizeInBytes)
	largeFileHandler.Write(rawData)
	largeFileHandler.Sync()

	return nil
}

func nameValidate(name string) error {
	if strings.Contains(name, ".") || strings.Contains(name, " ") || strings.Contains(name, "\t") ||
		strings.Contains(name, "\n") || strings.Contains(name, ":") || strings.Contains(name, "/") ||
		strings.Contains(name, "~") {
		return errors.New("object name must not contain space, '.', ':', '/', ~ ")
	}

	return nil
}
