package algo

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilterStringification(t *testing.T) {
	// Test empty bloom filter
	bf := NewEmptyBloomFilter(10)
	result := bf.String()
	expected := "0000000000" // 10 zeros
	assert.Equal(t, expected, result, "Empty bloom filter should return all zeros")
	assert.Len(t, result, 10, "Result should have correct length")

	// Test bloom filter with added items
	bf.Add("test")
	result = bf.String()

	// Should contain some 1s now
	assert.Len(t, result, 10, "Result should maintain correct length")
	assert.Contains(t, result, "1", "Result should contain at least one '1' after adding item")
	assert.NotEqual(t, "0000000000", result, "Result should be different from all zeros after adding item")

	// All characters should be 0 or 1
	for _, char := range result {
		assert.True(t, char == '0' || char == '1', "All characters should be 0 or 1, found: %c", char)
	}
}

func TestBloomFilterStringificationLarger(t *testing.T) {
	// Test with larger bloom filter (like in the example)
	bf := NewEmptyBloomFilter(100)
	result := bf.String()

	// Should be 100 zeros initially
	expected := strings.Repeat("0", 100)
	assert.Equal(t, expected, result, "Empty 100-bit bloom filter should return 100 zeros")
	assert.Len(t, result, 100, "Result should have 100 characters")

	// Add some items
	bf.Add("key1")
	bf.Add("key2")
	result = bf.String()

	// Should still be 100 characters but with some 1s
	assert.Len(t, result, 100, "Result should maintain 100 characters after adding items")
	assert.Contains(t, result, "1", "Result should contain at least one '1' after adding items")

	// Count 1s - should have at least 3 (since we use 3 hash functions)
	ones := strings.Count(result, "1")
	assert.GreaterOrEqual(t, ones, 3, "Should have at least 3 ones after adding 2 items (3 hash functions each)")
}

func TestBloomFilterStringificationConsistency(t *testing.T) {
	// Test that string representation is consistent
	bf := NewEmptyBloomFilter(50)
	bf.Add("consistent")

	result1 := bf.String()
	result2 := bf.String()

	assert.Equal(t, result1, result2, "String representation should be consistent across calls")
	assert.Len(t, result1, 50, "Result should have correct length")
}

func TestBloomFilterAddAndContains(t *testing.T) {
	bf := NewEmptyBloomFilter(1000)

	// Initially should not contain anything
	assert.False(t, bf.Contains("test"), "Empty bloom filter should not contain 'test'")

	// Add item
	bf.Add("test")
	assert.True(t, bf.Contains("test"), "Bloom filter should contain 'test' after adding it")

	// String should reflect the addition
	result := bf.String()
	assert.Len(t, result, 1000, "String should have correct length")
	assert.Contains(t, result, "1", "String should contain at least one '1' after adding item")
}

func TestBloomFilterFromExampleData(t *testing.T) {
	// Test with example data to understand the pattern
	bf := NewEmptyBloomFilter(100)
	bf.Add("key1")
	bf.Add("key2")

	result := bf.String()
	assert.Len(t, result, 100, "Result should be 100 characters")

	// The result should be deterministic for the same input
	bf2 := NewEmptyBloomFilter(100)
	bf2.Add("key1")
	bf2.Add("key2")
	result2 := bf2.String()

	assert.Equal(t, result, result2, "Same inputs should produce same bloom filter string")
}