package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparseIndexStringification(t *testing.T) {
	// Test empty sparse index
	si := NewSparseIndex()
	result := si.String()
	assert.Empty(t, result, "Empty sparse index should return empty string")

	// Test single entry
	si.Update("key1", 0)
	result = si.String()
	assert.Equal(t, "key1:0", result, "Single entry should format correctly")
	assert.Equal(t, len("key1:0"), len(result), "Result should have expected length")

	// Test multiple entries
	si.Update("key2", 38)
	si.Update("key3", 100)
	result = si.String()

	assert.Equal(t, "key1:0,key2:38,key3:100", result, "Result should have expected length")
}

func TestSparseIndexUpdate(t *testing.T) {
	si := NewSparseIndex()

	// Test adding new key
	si.Update("test", 42)
	assert.Equal(t, SparseIndexOffset(42), si.index["test"], "Should store correct offset for new key")

	// Test updating existing key
	si.Update("test", 100)
	assert.Equal(t, SparseIndexOffset(100), si.index["test"], "Should update existing key with new offset")

	// Check only one entry exists
	assert.Len(t, si.index, 1, "Should contain exactly one entry after updates")
}
