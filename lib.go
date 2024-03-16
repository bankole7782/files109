package files109

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

func getPartitionFiles(linuxFolderPath, partitionName string) (string, string) {
	return filepath.Join(linuxFolderPath, partitionName+".f109a"), filepath.Join(linuxFolderPath, partitionName+".f109b")
}

func CreatePartition(linuxFolderPath, partitionName string, indexPartionSizeInGBs, dataPartitionSizeInGBs int) error {
	// validate partition name
	err := nameValidate(partitionName)
	if err != nil {
		return err
	}

	indexPartitionPath, dataPartitionPath := getPartitionFiles(linuxFolderPath, partitionName)

	// make first partition
	// indexPartitionPath := filepath.Join(linuxFolderPath, partitionName+".f109a")
	indexPartionSizeInBytes := indexPartionSizeInGBs * int(math.Pow10(9))

	err = makeLargeFileEmpty(indexPartitionPath, indexPartionSizeInBytes)
	if err != nil {
		return err
	}

	// make second partition
	// dataPartitionPath := filepath.Join(linuxFolderPath, partitionName+".f109b")
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

func WriteFile(linuxFolderPath, partitionName, name string, data []byte) error {

	indexPtElems, err := ReadIndexPartition(linuxFolderPath, partitionName)
	if indexPtElems == nil {
		return err
	}

	usedSizeOfIndexPt := getWrittenIndexPartition(indexPtElems)

	_, dataPartitionPath := getPartitionFiles(linuxFolderPath, partitionName)
	dataPartitionHandle, err := os.OpenFile(dataPartitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer dataPartitionHandle.Close()

	_, err = dataPartitionHandle.Write(data)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	indexPtElem := IndexPartitionElem{FileName: name, DataBegin: usedSizeOfIndexPt,
		DataEnd: usedSizeOfIndexPt + int64(len(data))}

	indexPtElems = append(indexPtElems, indexPtElem)

	return rewriteIndexPartition(linuxFolderPath, partitionName, indexPtElems)
}

func rewriteIndexPartition(linuxFolderPath, partitionName string, elems []IndexPartitionElem) error {
	indexPartitionPath, _ := getPartitionFiles(linuxFolderPath, partitionName)

	indexPartitionHandle, err := os.OpenFile(indexPartitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer indexPartitionHandle.Close()

	out := IndexPartitionBegin
	for _, elem := range elems {
		tmp := fmt.Sprintf("data_key: %s\ndata_begin: %d\ndata_end:%d\n\n", elem.FileName,
			elem.DataBegin, elem.DataEnd)
		out += tmp
	}
	out += IndexPartitionEnd

	indexPartitionHandle.Write([]byte(out))
	return nil
}

func ReadIndexPartition(linuxFolderPath, partitionName string) ([]IndexPartitionElem, error) {
	emptyElems := make([]IndexPartitionElem, 0)

	sizeOfBeginStr := len(IndexPartitionBegin)
	indexPartitionPath, _ := getPartitionFiles(linuxFolderPath, partitionName)

	indexPartitionHandle, err := os.Open(indexPartitionPath)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}
	defer indexPartitionHandle.Close()

	firstBytes := make([]byte, sizeOfBeginStr)
	_, err = indexPartitionHandle.Read(firstBytes)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}

	if string(firstBytes) != IndexPartitionBegin {
		return emptyElems, nil
	}

	// read the raw header
	headerBytes := make([]byte, 0)
	for i := int64(sizeOfBeginStr); i < getFileSize(indexPartitionPath); i++ {
		aByte := make([]byte, 1)
		_, err = indexPartitionHandle.ReadAt(aByte, i)
		if err != nil {
			return nil, errors.Wrap(err, "os error")
		}
		if string(aByte) == "=" {
			endBytes := make([]byte, len(IndexPartitionEnd))
			_, err = indexPartitionHandle.ReadAt(endBytes, i)
			if err != nil {
				return nil, errors.Wrap(err, "os error")
			}

			if string(endBytes) == IndexPartitionEnd {
				break
			}
		} else {
			headerBytes = append(headerBytes, aByte...)
		}
	}

	fmt.Println(headerBytes)

	// parse the header
	return emptyElems, nil
}

func getFileSize(path string) int64 {
	stats, _ := os.Stat(path)
	return stats.Size()
}

func getWrittenIndexPartition(indexPartitionElems []IndexPartitionElem) int64 {
	if len(indexPartitionElems) == 0 {
		return 0
	}

	return indexPartitionElems[len(indexPartitionElems)-1].DataEnd
}
