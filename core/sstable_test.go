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

	filepath := fmt.Sprintf("%s/level_0/0001.sst", tempDir)

	assert.NoError(t, err)
	assert.FileExists(t, filepath)
	content, _ := os.ReadFile(filepath)
	assert.Equal(t, data, content)
}
