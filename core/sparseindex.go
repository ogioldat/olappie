package core

import (
	"fmt"
	"strings"
)

type (
	SparseIndexKey    string
	SparseIndexOffset int
)

type SparseIndex struct {
	index             map[SparseIndexKey]SparseIndexOffset
	offsetAccumulator int
}

func (si *SparseIndex) Update(key SparseIndexKey, offset SparseIndexOffset) {
	si.index[key] = SparseIndexOffset(si.offsetAccumulator)
	si.offsetAccumulator += int(offset)
}

func (si *SparseIndex) Get(key SparseIndexKey) (SparseIndexOffset, bool) {
	offset, exists := si.index[key]
	return offset, exists
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{
		index:             make(map[SparseIndexKey]SparseIndexOffset),
		offsetAccumulator: 0,
	}
}

func (si *SparseIndex) String() string {
	var result []string
	for key, offset := range si.index {
		result = append(result, fmt.Sprintf("%s:%d", key, offset))
	}
	return strings.Join(result, ",")
}
