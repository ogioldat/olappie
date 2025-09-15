package core

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddSSTable(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}

	manager := NewSSTableManager(cfg)
	manager.AddSSTable(cfg)

	assert.Equal(t, 1, len(manager.sstables))
	assert.Equal(t, "0001", manager.sstables[0][0].Name)
	assert.Equal(t, 0, manager.sstables[0][0].Level)
	assert.Len(t, manager.sstables, 1)
}

func TestAddManySSTables(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}

	manager := NewSSTableManager(cfg)

	manager.AddSSTable(cfg)
	manager.AddSSTable(cfg)
	manager.AddSSTable(cfg)
	manager.AddSSTable(cfg)

	assert.Equal(t, 4, len(manager.sstables[0]))
}

func TestSSTableWrite(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}

	manager := NewSSTableManager(cfg)

	manager.AddSSTable(cfg)
	manager.AddSSTable(cfg)

	sstable := manager.sstables[0][0]
	data := []byte("Hello, SSTable!")

	_, err := sstable.Write(data)

	filepath := fmt.Sprintf("%s/sstables/level_0/0001.sst", tempDir)

	assert.NoError(t, err)
	assert.FileExists(t, filepath)
	content, _ := os.ReadFile(filepath)
	assert.Equal(t, data, content)
}

func TestSSTableManagerFilePath(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}
	manager := NewSSTableManager(cfg)

	expectedPath := fmt.Sprintf("%s/sstables/level_1/test.sst", tempDir)
	actualPath := manager.FilePath("test", 1)
	assert.Equal(t, expectedPath, actualPath)
}

func TestSSTableManagerAddMultipleLevels(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}
	manager := NewSSTableManager(cfg)

	sstable1 := manager.AddSSTable(cfg)
	sstable2 := manager.AddSSTable(cfg)
	sstable3 := manager.AddSSTable(cfg)

	assert.Equal(t, "0001", sstable1.Name)
	assert.Equal(t, "0002", sstable2.Name)
	assert.Equal(t, "0003", sstable3.Name)

	assert.Equal(t, 0, sstable1.Level)
	assert.Equal(t, 0, sstable2.Level)
	assert.Equal(t, 0, sstable3.Level)

	assert.Equal(t, 3, len(manager.sstables[0]))
}

func TestSSTableBloomFilter(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}
	manager := NewSSTableManager(cfg)

	sstable := manager.AddSSTable(cfg)
	assert.NotNil(t, sstable.BloomFilter)

	sstable.BloomFilter.Add("test_key")
	assert.True(t, sstable.BloomFilter.Contains("test_key"))
	assert.False(t, sstable.BloomFilter.Contains("non_existent"))
}

func TestMetadataSet(t *testing.T) {
	metadata := NewMetadata()

	metadata.Set("table1", "a", "z", 0)
	metadata.Set("table2", "m", "p", 1)

	assert.Equal(t, 2, len(metadata.sstables))
	
	entry1 := metadata.sstables[SSTableId("table1")]
	assert.Equal(t, "a", entry1.minKey)
	assert.Equal(t, "z", entry1.maxKey)
	assert.Equal(t, 0, entry1.level)

	entry2 := metadata.sstables[SSTableId("table2")]
	assert.Equal(t, "m", entry2.minKey)
	assert.Equal(t, "p", entry2.maxKey)
	assert.Equal(t, 1, entry2.level)
}

func TestMetadataFlushAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	cfg := LSMTStorageConfig{outputDir: tempDir}

	metadata := NewMetadata()
	metadata.Set("table1", "a", "z", 0)
	metadata.Set("table2", "m", "p", 1)

	err := metadata.Flush(cfg)
	assert.NoError(t, err)

	metadataFile := fmt.Sprintf("%s/metadata", tempDir)
	assert.FileExists(t, metadataFile)

	newMetadata := NewMetadata()
	err = newMetadata.Load(cfg)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(newMetadata.sstables))
}

func TestSSTableManagerFindByKey(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}
	manager := NewSSTableManager(cfg)
	metadata := NewMetadata()

	sstable := manager.AddSSTable(cfg)
	metadata.Set(sstable.Name, "a", "z", 0)

	found := manager.FindByKey("m", metadata)
	assert.Nil(t, found)

	found = manager.FindByKey("nonexistent", metadata)
	assert.Nil(t, found)
}

func TestSSTableId(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &LSMTStorageConfig{outputDir: tempDir}
	manager := NewSSTableManager(cfg)

	sstable := manager.AddSSTable(cfg)
	expectedId := SSTableId("0_0001")
	assert.Equal(t, expectedId, sstable.Id)
}
