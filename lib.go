package files109

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func CreatePartition(linuxFolderPath, partitionName string, sizeInGBs int) error {
	// validate partition name
	err := nameValidate(partitionName)
	if err != nil {
		return err
	}

	// make partition
	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionSizeInBytes := sizeInGBs * int(math.Pow10(9))

	err = makeLargeFileEmpty(partitionPath, partitionSizeInBytes)
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
	indexPtElems, err := ReadAllFiles(linuxFolderPath, partitionName)
	if indexPtElems == nil {
		return err
	}

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")

	usedSizeOfIndexPt := getWrittenIndexPartition(indexPtElems)
	dataEnd := usedSizeOfIndexPt + int64(len(data)) + 1
	partitionSize := getFileSize(partitionPath)
	if dataEnd > partitionSize {
		return errors.New("you have reached the end of the partition")
	}

	partitionHandle, err := os.OpenFile(partitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	// delete old file contents if exists
	var fileElem IndexPartitionElem
	for _, elem := range indexPtElems {
		if elem.FileName == name {
			fileElem = elem
			break
		}
	}

	if fileElem.FileName != "" {
		emptyData := make([]byte, fileElem.DataEnd-fileElem.DataBegin)
		partitionHandle.WriteAt(emptyData, fileElem.DataBegin)
	}

	_, err = partitionHandle.WriteAt(data, usedSizeOfIndexPt+1)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	indexPtElem := IndexPartitionElem{FileName: name, DataBegin: usedSizeOfIndexPt + 1,
		DataEnd: dataEnd}

	indexPtElems = append(indexPtElems, indexPtElem)

	return rewriteIndexAtBase(linuxFolderPath, partitionName, indexPtElems)
}

func rewriteIndexAtBase(linuxFolderPath, partitionName string, elems []IndexPartitionElem) error {
	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDWR, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	out := IndexPartitionBegin
	for _, elem := range elems {
		tmp := fmt.Sprintf("data_key: %s\ndata_begin: %d\ndata_end:%d\n\n", elem.FileName,
			elem.DataBegin, elem.DataEnd)
		out += tmp
	}
	out += IndexPartitionEnd

	partitionSize := getFileSize(partitionPath)

	indexOffset := partitionSize - int64(len(out))
	partitionHandle.WriteAt([]byte(out), indexOffset)
	return nil
}

func ReadAllFiles(linuxFolderPath, partitionName string) ([]IndexPartitionElem, error) {
	emptyElems := make([]IndexPartitionElem, 0)

	sizeOfBeginStr := len(IndexPartitionBegin)

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionSize := getFileSize(partitionPath)

	sizeOfEndStr := len(IndexPartitionEnd)

	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	lastBytes := make([]byte, len(IndexPartitionEnd))
	_, err = partitionHandle.ReadAt(lastBytes, partitionSize-int64(sizeOfEndStr))
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}

	if string(lastBytes) != IndexPartitionEnd {
		return emptyElems, nil
	}

	// read the raw header
	indexesBytes := make([]byte, 0)
	for i := int64(1); i < partitionSize; i++ {
		// fmt.Println(i)
		aByte := make([]byte, 1)
		readOffset := partitionSize - int64(sizeOfEndStr) - i
		_, err = partitionHandle.ReadAt(aByte, readOffset)
		if err != nil {
			return nil, errors.Wrap(err, "os error")
		}
		if string(aByte) == "=" {
			endBytes := make([]byte, sizeOfBeginStr)
			partitionHandle.ReadAt(endBytes, readOffset-int64(sizeOfBeginStr)+1)

			if string(endBytes) == IndexPartitionBegin {
				break
			}
		} else {
			indexesBytes = append(indexesBytes, aByte...)
		}
	}
	slices.Reverse(indexesBytes)
	// parse the header
	ret, _ := parseIndexesString(string(indexesBytes))
	return ret, nil
}

func parseIndexesString(load string) ([]IndexPartitionElem, error) {
	ret := make([]IndexPartitionElem, 0)

	tmpMap := make(map[string]IndexPartitionElem)
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
		tmpMap[elem.FileName] = elem
	}

	for _, elem := range tmpMap {
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

	largestByteEnd := int64(0)
	for _, elem := range indexPartitionElems {
		if elem.DataEnd > largestByteEnd {
			largestByteEnd = elem.DataEnd
		}
	}

	return largestByteEnd
}

func ReadFile(linuxFolderPath, partitionName, name string) ([]byte, error) {
	elems, err := ReadAllFiles(linuxFolderPath, partitionName)
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

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	partitionHandle.ReadAt(fileData, fileElem.DataBegin)

	return fileData, nil
}

func DeleteFile(linuxFolderPath, partitionName, name string) error {
	elems, err := ReadAllFiles(linuxFolderPath, partitionName)
	if err != nil {
		return err
	}

	var elemIndex int
	var fileElem IndexPartitionElem
	for i, elem := range elems {
		if elem.FileName == name {
			fileElem = elem
			elemIndex = i
		}
	}

	fileData := make([]byte, fileElem.DataEnd-fileElem.DataBegin)

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")

	partitionHandle, err := os.OpenFile(partitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	partitionHandle.WriteAt(fileData, fileElem.DataBegin)

	newElems := slices.Delete(elems, elemIndex, elemIndex+1)

	return rewriteIndexAtBase(linuxFolderPath, partitionName, newElems)
}
