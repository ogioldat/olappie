package core

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/ogioldat/ttrunksdb/algo"
)

type SSTableManager struct {
	sstables     map[int][]*SSTable
	outputDir    string
	seqNumber    int
	serializer   SSTableSerializer
	deserializer SSTableDeserializer
}

type SSTable struct {
	Level       int
	Name        string
	Path        string
	BloomFilter *algo.BloomFilter
	SparseIndex *algo.SparseIndex
	CreatedAt   time.Time
	seqNumber   int
}

func (s *SSTable) AllKeys() []string {
	// TODO List
	return []string{"orange", "apple"}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func NewSSTableManager(config *LSMTStorageConfig) *SSTableManager {
	manager := &SSTableManager{
		sstables:     make(map[int][]*SSTable),
		outputDir:    path.Join(config.outputDir, "sstables"),
		seqNumber:    0,
		serializer:   &BinarySSTableSerializer{},
		deserializer: &BinarySSTableDeserializer{},
	}

	return manager
}

func (m *SSTableManager) FilePath(name string, level int) string {
	return path.Join(m.outputDir, "level_"+fmt.Sprint(level), name+".bin")
}

func (m *SSTableManager) AddSSTable(config *LSMTStorageConfig) *SSTable {
	level := 0
	nextName := fmt.Sprintf("%04d", len(m.sstables[level])+1)
	sstable := &SSTable{
		Level:       level,
		Name:        nextName,
		Path:        m.FilePath(nextName, level),
		BloomFilter: algo.NewEmptyBloomFilter(config.sstableBloomFilterSize),
		CreatedAt:   time.Now(),
		seqNumber:   m.seqNumber,
		SparseIndex: algo.NewSparseIndex(),
	}
	m.sstables[level] = append(m.sstables[level], sstable)
	m.seqNumber++

	return sstable
}

func (m *SSTableManager) Read(s *SSTable, key string) (*DBRecord, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	metadataOffset := m.serializer.MetadataSize(*s.BloomFilter, *s.SparseIndex)

	offset, exists := s.SparseIndex.Get(algo.SparseIndexKey(key))
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	reader := bufio.NewReader(file)
	// Advance the reader to the record's offset
	_, err = reader.Discard(metadataOffset + int(offset))

	if err != nil {
		return nil, err
	}

	deserialized, err := m.deserializer.DeserializeRecord(reader)

	return deserialized, err
}

func (m *SSTableManager) Flush(s *SSTable, memtable MemTable) error {
	dir := path.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	file, err := os.Create(s.Path)
	if err != nil {
		return err
	}

	defer file.Close()

	records := []DBRecord{}
	byteOffset := 0

	for kv := range memtable.Iterator() {
		records = append(records, DBRecord{
			Key:       DBRecordKey(kv.Key),
			Value:     kv.Value,
			Timestamp: DBRecordTimestamp(kv.Metadata.Timestamp.Unix()),
			Tombstone: false,
		})

		s.BloomFilter.Add(kv.Key)
		s.SparseIndex.Update(
			algo.SparseIndexKey(kv.Key),
			algo.SparseIndexOffset(byteOffset),
		)

		byteOffset += m.serializer.RecordSize(DBRecordKey(kv.Key), kv.Value)
	}

	serialized, err := m.serializer.Serialize(
		*s.BloomFilter,
		*s.SparseIndex,
		records,
	)

	if err != nil {
		return err
	}

	_, err = file.Write(serialized)

	return err
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
