package files109

type IndexPartitionElem struct {
	FileName  string
	DataBegin int64
	DataEnd   int64
}

const (
	IndexPartitionEnd   = "===END==="
	IndexPartitionBegin = "===BEGIN==="
)
