package files109

import "sync"

type IndexElem struct {
	FileName  string
	DataBegin int64
	DataEnd   int64
}

const (
	IndexEnd   = "===END==="
	IndexBegin = "===BEGIN==="
)

var mutex sync.RWMutex
