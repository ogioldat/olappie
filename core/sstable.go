package core

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/ogioldat/olappie/algo"
)

type SSTableId string

type SSTable struct {
	Level       int
	Name        string
	Path        string
	Id          SSTableId
	BloomFilter *algo.BloomFilter
	SparseIndex *SparseIndex
	CreatedAt   time.Time
	seqNumber   int
}

func (s *SSTable) AllKeys() []string {
	// TODO List
	return []string{"orange", "apple"}
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

	defer file.Close()

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
	seqNumber int
}

func NewSSTableManager(config *LSMTStorageConfig) *SSTableManager {
	manager := &SSTableManager{
		sstables:  make(map[int][]*SSTable),
		outputDir: path.Join(config.outputDir, "sstables"),
		seqNumber: 0,
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
		Level:       level,
		Name:        nextName,
		Path:        m.FilePath(nextName, level),
		BloomFilter: algo.NewBloomFilter(config.sstableBloomFilterSize),
		Id:          SSTableId(id(nextName, level)),
		CreatedAt:   time.Now(),
		seqNumber:   m.seqNumber,
		SparseIndex: NewSparseIndex(),
	}
	m.sstables[level] = append(m.sstables[level], sstable)
	m.seqNumber++

	return sstable
}

func (m *SSTableManager) FindByKey(key string) *SSTable {
	// Find most recent SSTable from level 0
	// TODO: Support multiple levels
	level := 0
	var sstables []*SSTable

	for _, sstable := range m.sstables[level] {
		if sstable.BloomFilter.Contains(key) {
			sstables = append(sstables, sstable)
		}
	}

	if len(sstables) == 0 {
		return nil
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].seqNumber > sstables[j].seqNumber
	})

	return sstables[0]
}
