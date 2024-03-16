package files109

type IndexElem struct {
	FileName  string
	DataBegin int64
	DataEnd   int64
}

const (
	IndexEnd   = "===END==="
	IndexBegin = "===BEGIN==="
)
