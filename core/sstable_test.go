package core

import (
	"fmt"
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
