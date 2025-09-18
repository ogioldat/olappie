package core

type (
	sparseIndexKey    string
	sparseIndexOffset int
	SparseIndex       map[sparseIndexKey]sparseIndexOffset
)

func (si *SparseIndex) Update(key sparseIndexKey, offset sparseIndexOffset) {
	(*si)[key] = offset
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{}
}
