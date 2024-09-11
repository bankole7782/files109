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
	indexElems, err := ReadAllFiles(linuxFolderPath, partitionName)
	if indexElems == nil {
		return err
	}

	mutex.Lock()
	defer mutex.Unlock()

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")

	indexesBeginOffset, err := findIndexesBeginOffset(linuxFolderPath, partitionName)
	if err != nil {
		return err
	}

	lastFileBytesEnd := getLastFileBytesEnd(indexElems)
	dataEnd := lastFileBytesEnd + int64(len(data)) + 1
	if dataEnd >= indexesBeginOffset {
		return errors.New("you have reached the end of the partition")
	}

	partitionHandle, err := os.OpenFile(partitionPath, os.O_WRONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	// delete old file contents if exists
	var fileElem IndexElem
	for _, elem := range indexElems {
		if elem.FileName == name {
			fileElem = elem
			break
		}
	}

	if fileElem.FileName != "" {
		emptyData := make([]byte, fileElem.DataEnd-fileElem.DataBegin)
		partitionHandle.WriteAt(emptyData, fileElem.DataBegin)
	}

	_, err = partitionHandle.WriteAt(data, lastFileBytesEnd+1)
	if err != nil {
		return errors.Wrap(err, "os error")
	}

	indexPtElem := IndexElem{FileName: name, DataBegin: lastFileBytesEnd + 1,
		DataEnd: dataEnd}

	indexElems = append(indexElems, indexPtElem)

	return rewriteIndexAtBase(linuxFolderPath, partitionName, indexElems)
}

func rewriteIndexAtBase(linuxFolderPath, partitionName string, elems []IndexElem) error {
	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDWR, 0666)
	if err != nil {
		return errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	out := IndexBegin
	for _, elem := range elems {
		tmp := fmt.Sprintf("%s\n%d\n%d\n\n", elem.FileName,
			elem.DataBegin, elem.DataEnd)
		out += tmp
	}
	out += IndexEnd

	partitionSize := getFileSize(partitionPath)

	indexOffset := partitionSize - int64(len(out))
	partitionHandle.WriteAt([]byte(out), indexOffset)
	return nil
}

func findIndexesBeginOffset(linuxFolderPath, partitionName string) (int64, error) {
	sizeOfBeginStr := len(IndexBegin)

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionSize := getFileSize(partitionPath)

	sizeOfEndStr := len(IndexEnd)

	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDWR, 0666)
	if err != nil {
		return 0, errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	lastBytes := make([]byte, len(IndexEnd))
	_, err = partitionHandle.ReadAt(lastBytes, partitionSize-int64(sizeOfEndStr))
	if err != nil {
		return 0, errors.Wrap(err, "os error")
	}

	if string(lastBytes) != IndexEnd {
		return 0, nil
	}

	endOffset := int64(0)
	for i := int64(1); i < partitionSize; i++ {
		// fmt.Println(i)
		aByte := make([]byte, 1)
		readOffset := partitionSize - int64(sizeOfEndStr) - i
		_, err = partitionHandle.ReadAt(aByte, readOffset)
		if err != nil {
			return 0, errors.Wrap(err, "os error")
		}
		if string(aByte) == "=" {
			endBytes := make([]byte, sizeOfBeginStr)
			partitionHandle.ReadAt(endBytes, readOffset-int64(sizeOfBeginStr)+1)

			if string(endBytes) == IndexBegin {
				endOffset = readOffset - int64(sizeOfBeginStr+1)
				break
			}
		}
	}

	return endOffset, nil
}

func ReadAllFiles(linuxFolderPath, partitionName string) ([]IndexElem, error) {
	mutex.RLock()
	defer mutex.RUnlock()
	emptyElems := make([]IndexElem, 0)

	sizeOfBeginStr := len(IndexBegin)

	partitionPath := filepath.Join(linuxFolderPath, partitionName+".f109")
	partitionSize := getFileSize(partitionPath)

	sizeOfEndStr := len(IndexEnd)

	partitionHandle, err := os.OpenFile(partitionPath, os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}
	defer partitionHandle.Close()

	lastBytes := make([]byte, len(IndexEnd))
	_, err = partitionHandle.ReadAt(lastBytes, partitionSize-int64(sizeOfEndStr))
	if err != nil {
		return nil, errors.Wrap(err, "os error")
	}

	if string(lastBytes) != IndexEnd {
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

			if string(endBytes) == IndexBegin {
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

func parseIndexesString(load string) ([]IndexElem, error) {
	ret := make([]IndexElem, 0)

	tmpMap := make(map[string]IndexElem)
	cleanedF1File := strings.ReplaceAll(string(load), "\r", "")
	partsOfRawF1File := strings.Split(cleanedF1File, "\n\n")
	for _, part := range partsOfRawF1File {
		innerParts := strings.Split(strings.TrimSpace(part), "\n")

		if len(innerParts) != 3 {
			continue
		}

		var elem IndexElem

		elem.FileName = innerParts[0]
		data, err := strconv.ParseInt(innerParts[1], 10, 64)
		if err != nil {
			return ret, errors.New("data_begin is not of type int64")
		}
		elem.DataBegin = data
		data2, err := strconv.ParseInt(innerParts[2], 10, 64)
		if err != nil {
			return ret, errors.New("data_end is not of type int64")
		}
		elem.DataEnd = data2

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

func getLastFileBytesEnd(IndexElems []IndexElem) int64 {
	if len(IndexElems) == 0 {
		return 0
	}

	largestByteEnd := int64(0)
	for _, elem := range IndexElems {
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

	mutex.RLock()
	defer mutex.RUnlock()

	var fileElem IndexElem
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

	mutex.Lock()
	defer mutex.Unlock()

	var elemIndex int
	var fileElem IndexElem
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
