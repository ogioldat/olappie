package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadFromKV(t *testing.T) {
	kvText := "aaa:123,bbb:456"

	sstable, _ := NewFromKVPairs(kvText)

	assert.Equal(t, 2, sstable.Size())
	assert.Equal(t, "aaa", sstable.First().Key)
	assert.Equal(t, "bbb", sstable.Last().Key)
}

func TestRBMemTableWrite(t *testing.T) {
	memTable := NewRBMemTable()

	err := memTable.Write("key1", []byte("value1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, memTable.Size())

	err = memTable.Write("key2", []byte("value2"))
	assert.NoError(t, err)
	assert.Equal(t, 2, memTable.Size())
}

func TestRBMemTableRead(t *testing.T) {
	memTable := NewRBMemTable()

	memTable.Write("test_key", []byte("test_value"))

	value, ok := memTable.Read("test_key")
	assert.True(t, ok)
	assert.Equal(t, []byte("test_value"), value)

	_, ok = memTable.Read("non_existent")
	assert.False(t, ok)
}

func TestRBMemTableReset(t *testing.T) {
	memTable := NewRBMemTable()

	memTable.Write("key1", []byte("value1"))
	memTable.Write("key2", []byte("value2"))
	assert.Equal(t, 2, memTable.Size())

	memTable.Reset()
	assert.Equal(t, 0, memTable.Size())

	_, ok := memTable.Read("key1")
	assert.False(t, ok)
}

func TestRBMemTableFirstLast(t *testing.T) {
	memTable := NewRBMemTable()

	memTable.Write("b", []byte("value_b"))
	memTable.Write("a", []byte("value_a"))
	memTable.Write("c", []byte("value_c"))

	first := memTable.First()
	assert.Equal(t, "a", first.Key)
	assert.Equal(t, []byte("value_a"), first.Value)

	last := memTable.Last()
	assert.Equal(t, "c", last.Key)
	assert.Equal(t, []byte("value_c"), last.Value)
}

func TestRBMemTableIterator(t *testing.T) {
	memTable := NewRBMemTable()

	memTable.Write("b", []byte("value_b"))
	memTable.Write("a", []byte("value_a"))
	memTable.Write("c", []byte("value_c"))

	var keys []string
	var values [][]byte

	for kv := range memTable.Iterator() {
		keys = append(keys, kv.Key)
		values = append(values, kv.Value)
	}

	assert.Equal(t, []string{"a", "b", "c"}, keys)
	assert.Equal(t, [][]byte{[]byte("value_a"), []byte("value_b"), []byte("value_c")}, values)
}

func TestNewFromKVPairsError(t *testing.T) {
	kvText := "invalid_format"

	memTable, err := NewFromKVPairs(kvText)
	assert.NoError(t, err)
	assert.Equal(t, 0, memTable.Size())
}

func TestNewFromKVPairsEmpty(t *testing.T) {
	kvText := ""

	memTable, err := NewFromKVPairs(kvText)
	assert.NoError(t, err)
	assert.Equal(t, 0, memTable.Size())
}

func TestRBMemTableOverwrite(t *testing.T) {
	memTable := NewRBMemTable()

	memTable.Write("key1", []byte("value1"))
	memTable.Write("key1", []byte("value1_updated"))

	value, ok := memTable.Read("key1")
	assert.True(t, ok)
	assert.Equal(t, []byte("value1_updated"), value)
	assert.Equal(t, 1, memTable.Size())
}
