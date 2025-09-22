package algo

import (
	"fmt"
	"strconv"
	"strings"
)

type (
	SparseIndexKey    string
	SparseIndexOffset int64
)

type SparseIndex struct {
	Index             map[SparseIndexKey]SparseIndexOffset
	offsetAccumulator int64
}

func (si *SparseIndex) Update(key SparseIndexKey, offset SparseIndexOffset) {
	si.Index[key] = SparseIndexOffset(offset)
	// si.offsetAccumulator += int64(offset)
}

func (si *SparseIndex) Get(key SparseIndexKey) (SparseIndexOffset, bool) {
	offset, exists := si.Index[key]
	return offset, exists
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{
		Index:             make(map[SparseIndexKey]SparseIndexOffset),
		offsetAccumulator: 0,
	}
}

func (si *SparseIndex) String() string {
	var result []string
	for key, offset := range si.Index {
		result = append(result, fmt.Sprintf("%s:%d", key, offset))
	}
	return strings.Join(result, ",")
}

func NewSparseIndexFromString(s string) *SparseIndex {
	si := NewSparseIndex()
	if s == "" {
		return si
	}
	entries := strings.SplitSeq(s, ",")

	for entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) != 2 {
			continue
		}
		key := SparseIndexKey(parts[0])
		offsetInt, err := strconv.ParseInt(parts[1], 10, 64)

		if err != nil {
			continue
		}

		offset := SparseIndexOffset(offsetInt)
		si.Index[key] = offset
	}
	return si
}
