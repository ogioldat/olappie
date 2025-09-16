package core

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ogioldat/olappie/algo"
)

type MetadataSStableEntry struct {
	minKey string
	maxKey string
	level  int
}

type Metadata struct {
	sstables map[SSTableId]MetadataSStableEntry
}

type SSTableId string

type SSTable struct {
	Size        int
	Level       int
	Name        string
	Path        string
	Id          SSTableId
	BloomFilter *algo.BloomFilter
	CreatedAt   time.Time
}

func (s *SSTable) Write(p []byte) (n int, err error) {
	dir := path.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}

	file, err := os.Create(s.Path)
	if err != nil {
		return 0, err
	}

	if err := file.Close(); err != nil {
		return 0, err
	}

	return file.Write(p)
}

func (s *SSTable) Read(key string) ([]byte, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	var value []byte
	if err := gob.NewDecoder(file).Decode(&value); err != nil {
		return nil, err
	}

	return value, nil
}

type SSTableManager struct {
	sstables  map[int][]*SSTable
	outputDir string
}

func NewSSTableManager(config *LSMTStorageConfig) *SSTableManager {
	manager := &SSTableManager{
		sstables:  make(map[int][]*SSTable),
		outputDir: path.Join(config.outputDir, "sstables"),
	}

	return manager
}

func (m *SSTableManager) FilePath(name string, level int) string {
	return path.Join(m.outputDir, "level_"+fmt.Sprint(level), name+".sst")
}

func id(name string, level int) string {
	return fmt.Sprintf("%d_%s", level, name)
}

func (m *SSTableManager) AddSSTable(config *LSMTStorageConfig) *SSTable {
	level := 0
	nextName := fmt.Sprintf("%04d", len(m.sstables[level])+1)
	sstable := &SSTable{
		Size:        0,
		Level:       level,
		Name:        nextName,
		Path:        m.FilePath(nextName, level),
		BloomFilter: algo.NewBloomFilter(1000000),
		Id:          SSTableId(id(nextName, level)),
		CreatedAt:   time.Now(),
	}
	m.sstables[level] = append(m.sstables[level], sstable)

	return sstable
}

func (m *SSTableManager) findLevelSSTables(key string, level int, metadata *Metadata) []*SSTable {
	var sstables []*SSTable

	// timeOrderedSSTables := metadata.sstables

	for _, met := range metadata.sstables {
		if met.maxKey <= key && met.minKey >= key {
			sstables = append(sstables, m.sstables[level]...)
		}
	}
	return sstables
}

func (m *SSTableManager) FindByKey(key string, metadata *Metadata) *SSTable {
	sstables := m.findLevelSSTables(key, 0, metadata)

	if len(sstables) == 0 {
		return nil
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].CreatedAt.After(sstables[j].CreatedAt)
	})

	return sstables[0]
}

func NewMetadata() *Metadata {
	return &Metadata{
		sstables: make(map[SSTableId]MetadataSStableEntry),
	}
}

func (m *Metadata) Set(tableName string, minKey string, maxKey string, level int) {
	k := SSTableId(tableName)
	m.sstables[k] = MetadataSStableEntry{
		minKey: minKey,
		maxKey: maxKey,
		level:  level,
	}
}

func (m *Metadata) Flush(config LSMTStorageConfig) error {
	metadataStr := ""
	for tableName, entry := range m.sstables {
		metadataStr += fmt.Sprintf(
			"%s %s %s\n", tableName, entry.minKey, entry.maxKey,
		)
	}

	file, err := os.OpenFile(
		path.Join(config.outputDir, "metadata"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return err
	}

	_, err = file.WriteString(metadataStr)
	if err != nil {
		return err
	}

	return file.Close()
}

func (m *Metadata) Load(config LSMTStorageConfig) error {
	dat, err := os.ReadFile(path.Join(config.outputDir, "metadata"))
	if err != nil {
		return nil
	}
	metadata := NewMetadata()
	lines := strings.Split(string(dat), "\n")

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) != 4 {
			continue
		}
		level, err := strconv.Atoi(parts[3])
		if err != nil {
			continue
		}
		metadata.Set(parts[0], parts[1], parts[2], level)
	}

	m.sstables = metadata.sstables

	return nil
}
