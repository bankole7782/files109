package files109

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
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

	_, dataPartitionPath := getPartitionFiles(linuxFolderPath, partitionName)

	usedSizeOfIndexPt := getWrittenIndexPartition(indexPtElems)
	dataEnd := usedSizeOfIndexPt + int64(len(data)) + 1
	dataPtSize := getFileSize(dataPartitionPath)
	if dataEnd > dataPtSize {
		return errors.New("you have reached the end of the partition")
	}

	dataPartitionHandle, err := os.OpenFile(dataPartitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer dataPartitionHandle.Close()

	_, err = dataPartitionHandle.WriteAt(data, usedSizeOfIndexPt+1)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	indexPtElem := IndexPartitionElem{FileName: name, DataBegin: usedSizeOfIndexPt + 1,
		DataEnd: dataEnd}

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

	// parse the header
	ret, _ := parseIndexPartitionString(string(headerBytes))
	return ret, nil
}

func parseIndexPartitionString(load string) ([]IndexPartitionElem, error) {
	ret := make([]IndexPartitionElem, 0)

	cleanedF1File := strings.ReplaceAll(string(load), "\r", "")
	partsOfRawF1File := strings.Split(cleanedF1File, "\n\n")
	for _, part := range partsOfRawF1File {
		innerParts := strings.Split(strings.TrimSpace(part), "\n")

		var elem IndexPartitionElem
		for _, line := range innerParts {
			var colonIndex int
			for i, ch := range line {
				if fmt.Sprintf("%c", ch) == ":" {
					colonIndex = i
					break
				}
			}

			if colonIndex == 0 {
				continue
			}

			optName := strings.TrimSpace(line[0:colonIndex])
			optValue := strings.TrimSpace(line[colonIndex+1:])

			if optName == "data_key" {
				elem.FileName = optValue
			} else if optName == "data_begin" {
				data, err := strconv.ParseInt(optValue, 10, 64)
				if err != nil {
					return ret, errors.New("data_begin is not of type int64")
				}
				elem.DataBegin = data
			} else if optName == "data_end" {
				data, err := strconv.ParseInt(optValue, 10, 64)
				if err != nil {
					return ret, errors.New("data_end is not of type int64")
				}
				elem.DataEnd = data
			}
		}

		if elem.FileName == "" {
			continue
		}
		ret = append(ret, elem)
	}

	return ret, nil
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

func ReadFile(linuxFolderPath, partitionName, name string) ([]byte, error) {
	elems, err := ReadIndexPartition(linuxFolderPath, partitionName)
	if err != nil {
		return nil, err
	}

	var fileElem IndexPartitionElem
	for _, elem := range elems {
		if elem.FileName == name {
			fileElem = elem
			break
		}
	}

	fileData := make([]byte, fileElem.DataEnd-fileElem.DataBegin)

	_, dataPartitionPath := getPartitionFiles(linuxFolderPath, partitionName)
	dataPartitionHandle, err := os.OpenFile(dataPartitionPath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}
	defer dataPartitionHandle.Close()

	dataPartitionHandle.ReadAt(fileData, fileElem.DataBegin)

	return fileData, nil
}
