package core

import (
	"github.com/ogioldat/olappie/algo"
)

type LSMTStorage struct {
	memTableThreshold int
	sparseIndex       SparseIndex
	memTable          MemTable
	ssTables          []SSTable
	wal               *WAL
}

func NewLSMTStorage(memTableThreshold int) *LSMTStorage {
	wal, err := NewWAL()
	if err != nil {
		panic("failed to create WAL")
	}

	return &LSMTStorage{
		memTableThreshold: memTableThreshold,
		sparseIndex:       make(SparseIndex),
		memTable: &RBMemTable{
			tree: algo.NewRBTree(),
		},
		ssTables: []SSTable{},
		wal:      wal,
	}
}

func (s *LSMTStorage) Write(key string, value string) error {
	if err := s.wal.Log(key, value); err != nil {
		return err
	}

	if err := s.memTable.Write(key, []byte(value)); err != nil {
		return err
	}

	if s.memTableThreshold < s.memTable.Size() {
		ssTable := NewSSTable()

		if err := s.memTable.Flush(ssTable); err != nil {
			return err
		}
	}

	return nil
}

func (s *LSMTStorage) Compact(key string) ([]byte, error) {
	return nil, nil
}
