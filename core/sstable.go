package core

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/ogioldat/olappie/algo"
)

type SSTableId string

type SSTableFile []byte

type Serializable struct {
	Key       string
	Value     []byte
	Timestamp int64
	Tombstone bool
}

type SSTableSerializer interface {
	Serialize(sstable SSTable, ser []Serializable) (SSTableFile, error)
	SerializeDataNode(node Serializable) (string, error)
}

type StandardSSTableSerializer struct{}

type SSTableManager struct {
	sstables   map[int][]*SSTable
	outputDir  string
	seqNumber  int
	serializer SSTableSerializer
}

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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func NewSSTableManager(config *LSMTStorageConfig) *SSTableManager {
	manager := &SSTableManager{
		sstables:   make(map[int][]*SSTable),
		outputDir:  path.Join(config.outputDir, "sstables"),
		seqNumber:  0,
		serializer: &StandardSSTableSerializer{},
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
		BloomFilter: algo.NewEmptyBloomFilter(config.sstableBloomFilterSize),
		Id:          SSTableId(id(nextName, level)),
		CreatedAt:   time.Now(),
		seqNumber:   m.seqNumber,
		SparseIndex: NewSparseIndex(),
	}
	m.sstables[level] = append(m.sstables[level], sstable)
	m.seqNumber++

	return sstable
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

	serializables := []Serializable{}
	for kv := range memtable.Iterator() {
		serializables = append(serializables, Serializable{
			Key:       kv.Key,
			Value:     kv.Value,
			Timestamp: kv.Metadata.Timestamp.Unix(),
			Tombstone: false,
		})
	}

	serialized, err := m.serializer.Serialize(*s, serializables)
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

func (s *StandardSSTableSerializer) Serialize(sstable SSTable, ser []Serializable) (SSTableFile, error) {
	bloomFilterStr := sstable.BloomFilter.String()
	sparseIndexStr := sstable.SparseIndex.String()

	var dataBlock []string

	for _, node := range ser {
		serializedNode, err := s.SerializeDataNode(node)
		if err != nil {
			return nil, err
		}
		dataBlock = append(dataBlock, serializedNode)
	}

	return []byte(bloomFilterStr + "\n" + sparseIndexStr + "\n" + strings.Join(dataBlock, ",") + "\n"), nil
}

func (s *StandardSSTableSerializer) SerializeDataNode(node Serializable) (string, error) {
	return fmt.Sprintf(
			"%d %s %d %s %d %d %d %d",
			len(node.Key), node.Key,
			len(node.Value), node.Value,
			8, node.Timestamp,
			1, boolToInt(node.Tombstone)),
		nil
}

func (s *StandardSSTableSerializer) Deserialize(data SSTableFile) ([]Serializable, error) {
	return nil, nil
}
