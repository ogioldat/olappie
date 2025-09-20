package algo

import (
	"hash/fnv"
)

type BloomFilter struct {
	bits []bool
	size int
}

func NewEmptyBloomFilter(size int) *BloomFilter {
	return &BloomFilter{
		bits: make([]bool, size),
		size: size,
	}
}

func (bf *BloomFilter) hash1(data string) int {
	h := fnv.New32a()
	h.Write([]byte(data))
	return int(h.Sum32()) % bf.size
}

func (bf *BloomFilter) hash2(data string) int {
	h := fnv.New32a()
	h.Write([]byte(data + "salt"))
	return int(h.Sum32()) % bf.size
}

func (bf *BloomFilter) hash3(data string) int {
	h := fnv.New32a()
	h.Write([]byte("prefix" + data))
	return int(h.Sum32()) % bf.size
}

func (bf *BloomFilter) Add(item string) {
	bf.bits[bf.hash1(item)] = true
	bf.bits[bf.hash2(item)] = true
	bf.bits[bf.hash3(item)] = true
}

func (bf *BloomFilter) Contains(item string) bool {
	return bf.bits[bf.hash1(item)] &&
		bf.bits[bf.hash2(item)] &&
		bf.bits[bf.hash3(item)]
}

func (bf *BloomFilter) String() string {
	result := ""
	for _, bit := range bf.bits {
		if bit {
			result += "1"
		} else {
			result += "0"
		}
	}
	return result
}
