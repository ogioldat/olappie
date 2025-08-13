package core

type sparseIndexKey string
type sparseIndexOffset int
type SparseIndex map[sparseIndexKey]sparseIndexOffset

func (si SparseIndex) Update(key sparseIndexKey, offset sparseIndexOffset) {
	
}
