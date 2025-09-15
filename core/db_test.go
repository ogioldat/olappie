package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBRead(t *testing.T) {
	tempDir := t.TempDir()

	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(3))

	db.Write("a", []byte("data"))
	db.Write("b", []byte("data"))
	db.Write("c", []byte("data"))
	db.Write("d", []byte("data"))

	assert.Equal(t, 1, db.memTable.Size())
	assert.Equal(t, db.memTable.First().Key, "d")
}

func TestDBWrite(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(5))

	err := db.Write("test_key", []byte("test_value"))
	assert.NoError(t, err)
	assert.Equal(t, 1, db.memTable.Size())

	value, ok := db.memTable.Read("test_key")
	assert.True(t, ok)
	assert.Equal(t, []byte("test_value"), value)
}

func TestDBWriteMultiple(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(10))

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4"), []byte("val5")}

	for i, key := range keys {
		err := db.Write(key, values[i])
		assert.NoError(t, err)
	}

	assert.Equal(t, 5, db.memTable.Size())

	for i, key := range keys {
		value, ok := db.memTable.Read(key)
		assert.True(t, ok)
		assert.Equal(t, values[i], value)
	}
}

func TestDBReadAfterWrite(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(10))

	key := "test_key"
	expectedValue := []byte("test_value")

	err := db.Write(key, expectedValue)
	assert.NoError(t, err)

	actualValue, err := db.Read(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, actualValue)
}

func TestDBReadNonExistentKey(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(10))

	_, err := db.Read("non_existent_key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sstable not found")
}

func TestDBMemTableFlush(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(3))

	db.Write("a", []byte("value_a"))
	db.Write("b", []byte("value_b"))
	assert.Equal(t, 2, db.memTable.Size())

	db.Write("c", []byte("value_c"))
	assert.Equal(t, 3, db.memTable.Size())

	db.Write("d", []byte("value_d"))
	assert.Equal(t, 1, db.memTable.Size())
	assert.Equal(t, "d", db.memTable.First().Key)
}

func TestDBReadFromSSTable(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(2))

	db.Write("a", []byte("value_a"))
	db.Write("b", []byte("value_b"))
	db.Write("c", []byte("value_c"))

	value, err := db.Read("a")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_a"), value)

	value, err = db.Read("b")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_b"), value)
}

func TestDBSequenceNumber(t *testing.T) {
	tempDir := t.TempDir()
	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(10))

	initialSeq := db.seqNumber

	db.Write("key1", []byte("value1"))
	assert.Equal(t, initialSeq+1, db.seqNumber)

	db.Write("key2", []byte("value2"))
	assert.Equal(t, initialSeq+2, db.seqNumber)
}

func TestDBConfigOptions(t *testing.T) {
	tempDir := t.TempDir()

	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(100))
	assert.Equal(t, 100, db.config.memTableThreshold)
	assert.Equal(t, tempDir, db.config.outputDir)

	db2 := NewLSMTStorage()
	assert.Equal(t, 1000, db2.config.memTableThreshold)
	assert.Equal(t, DEFAULT_OUTPUT_DIR, db2.config.outputDir)
}
