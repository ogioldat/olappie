package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadFromKV(t *testing.T) {
	kvText := "aaa:123,bbb:456"

	sstable := NewFromKVPairs(kvText)

	assert.Equal(t, 2, sstable.Size())
	assert.Equal(t, "aaa", sstable.First().Key)
	assert.Equal(t, "bbb", sstable.Last().Key)

}
