package olappie

import (
	"unsafe"

	"github.com/ogioldat/olappie/olappie/algo"
)

type sparseIndexKey string
type sparseIndexOffset int
type sparseIndex map[sparseIndexKey]sparseIndexOffset

type SSTable struct{}

type memTable interface {
	Write(string, []byte) error
	Flush() ([]SSTable, error)
	Size() int
}

type RBMemTable struct {
	tree *algo.RBTree
}

func (r *RBMemTable) Write(key string, value []byte) error {
	return nil
}

func (r *RBMemTable) Flush() ([]SSTable, error) {
	return nil, nil
}

func (r *RBMemTable) Size() int {
	return int(unsafe.Sizeof(*r.tree))
}

type LSMTStorage struct {
	memTableThreshold int
	sparseIndex       sparseIndex
	memTable          memTable
	ssTables          []SSTable
}

func NewLSMTStorage(memTableThreshold int) *LSMTStorage {
	return &LSMTStorage{
		memTableThreshold: memTableThreshold,
		sparseIndex:       make(sparseIndex),
		memTable: &RBMemTable{
			tree: algo.NewRBTree(),
		},
		ssTables: []SSTable{},
	}
}

func (s *LSMTStorage) Write(key string, value string) error {
	err := s.memTable.Write(key, []byte(value))
	if err != nil {
		return err
	}

	if s.memTableThreshold < s.memTable.Size() {
		_, err := s.memTable.Flush()
		if err != nil {
			return err
		}
	}

	return nil
}
