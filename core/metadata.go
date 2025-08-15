package core

import (
	"fmt"
	"os"
	"strings"
)

const DEFAULT_METADATA_FILE = "../data/metadata"

type sstableEntry struct {
	minKey string
	maxKey string
}

type Metadata struct {
	sstables map[string]sstableEntry
}

func NewMetadata() *Metadata {
	return &Metadata{
		sstables: make(map[string]sstableEntry),
	}
}

func (m *Metadata) SetSSTable(key string, minKey string, maxKey string) {
	m.sstables[key] = sstableEntry{
		minKey: minKey,
		maxKey: maxKey,
	}
}

func (m *Metadata) Flush() error {
	metadataStr := ""
	for tableName, entry := range m.sstables {
		metadataStr += fmt.Sprintf(
			"%s %s %s\n", tableName, entry.minKey, entry.maxKey,
		)
	}

	file, err := os.OpenFile(DEFAULT_METADATA_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(metadataStr)
	if err != nil {
		return err
	}

	return nil
}

func (m *Metadata) Load() error {
	dat, err := os.ReadFile(DEFAULT_METADATA_FILE)
	if err != nil {
		return nil
	}

	metadata := NewMetadata()
	lines := strings.Split(string(dat), "\n")

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) != 3 {
			continue
		}
		metadata.SetSSTable(parts[0], parts[1], parts[2])
	}

	m.sstables = metadata.sstables

	return nil
}
