package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeMatchesExpectedFormat(t *testing.T) {
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 100,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)

	data := []Serializable{
		{Key: "a", Value: []byte("some value"), Timestamp: 1751374012},
		{Key: "bbbb", Value: []byte(""), Timestamp: 1751354012},
		{Key: "cc", Value: []byte("some other value"), Timestamp: 1751354015},
	}

	serialized, _ := manager.serializer.Serialize(*sstable, data)

	expected := `%s
%s

1 a 10 some value 8 1751374012 1 0,4 bbbb 0  8 1751354012 1 1,2 cc 16 some other value 8 1751354015 1 0
	`
	expected = fmt.Sprintf(expected, sstable.BloomFilter.String(), sstable.SparseIndex.String())

	fmt.Println("Serialized Result:", string(serialized))

	assert.Equal(t, serialized, expected, "Serialized result should match expected format")
}
