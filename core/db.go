package core

import (
	"fmt"
	"os"
	"slices"

	"github.com/ogioldat/olappie/internal"
)

const (
	_  = iota             // Use iota to create a sequence of constants
	KB = 1 << (10 * iota) // 1 << 10 (1024 bytes)
	MB = 1 << (10 * iota) // 1 << 20 (1024 * 1024 bytes)
	GB = 1 << (10 * iota) // 1 << 30
)

type DBRecordKey string
type DBRecordValue []byte
type DBRecordTimestamp int64
type DBRecordTombstone bool

type DBRecord struct {
	Key       DBRecordKey
	Value     DBRecordValue
	Timestamp DBRecordTimestamp
	Tombstone DBRecordTombstone
}

const MAX_SCALAR_SIZE = 1 * KB

type DB interface {
	Read(string) ([]byte, error)
	Write(string, []byte) error
	Iter(yield func(key string, value []byte) bool)
}

type Option func(*LSMTStorageConfig)

func WithOutDir(dir string) Option {
	return func(m *LSMTStorageConfig) {
		m.outputDir = dir
	}
}

func WithSSTableBloomFilterSize(size int) Option {
	return func(m *LSMTStorageConfig) {
		m.sstableBloomFilterSize = size
	}
}

func WithMemtableThreshold(th int) Option {
	return func(m *LSMTStorageConfig) {
		m.memTableThreshold = th
	}
}

type LSMTStorageConfig struct {
	memTableThreshold      int // Max size of entries in the memtable before flushing to SSTables
	outputDir              string
	sstableBloomFilterSize int
}

type LSMTStorage struct {
	config         *LSMTStorageConfig
	seqNumber      int
	memTable       MemTable
	ssTableManager *SSTableManager
	wal            *WAL
}

func NewLSMTStorage(opts ...Option) *LSMTStorage {
	var outputDir = os.Getenv("OLAPPIE_DATA_DIR")
	if outputDir == "" {
		panic("OLAPPIE_DATA_DIR environment variable is not set")
	}

	config := &LSMTStorageConfig{
		memTableThreshold:      1000,
		outputDir:              outputDir,
		sstableBloomFilterSize: 10000,
	}

	for _, opt := range opts {
		opt(config)
	}

	wal, err := NewWAL(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create WAL: %v", err))
	}

	return &LSMTStorage{
		config:         config,
		seqNumber:      0,
		memTable:       NewRBMemTable(),
		ssTableManager: NewSSTableManager(config),
		wal:            wal,
	}
}

func (s *LSMTStorage) updateSeq() {
	s.seqNumber++
}

func (s *LSMTStorage) Write(key string, value []byte) error {
	if len(value) > MAX_SCALAR_SIZE {
		return fmt.Errorf("value size exceeds maximum allowed size of %d bytes", MAX_SCALAR_SIZE)
	}

	err := s.wal.Log(key, string(value))

	if err != nil {
		internal.Logger.Debug("WAL log failed", "key", key, "value", value, "err", err)
		return err
	}

	if err := s.memTable.Append(key, []byte(value)); err != nil {
		internal.Logger.Debug("Memtable write failed", "key", key, "value", value, "err", err)
		return err
	}

	internal.Logger.Debug("Write to memtable", "key", key, "value", value)

	s.updateSeq()

	// TODO: Move as a background task
	if s.config.memTableThreshold <= s.memTable.Size() {
		sstable := s.ssTableManager.AddSSTable(s.config)
		if err := s.ssTableManager.Flush(sstable, s.memTable); err != nil {
			internal.Logger.Debug("Memtable flush failed", "sstable", sstable.Name, "err", err)
			return err
		}
		internal.Logger.Debug("Memtable flushed to SSTable", "sstable", sstable.Name)
		s.memTable.Reset()
	}

	return nil
}

func (s *LSMTStorage) Compact(key string) ([]byte, error) {
	return nil, nil
}

func (s *LSMTStorage) Read(key string) ([]byte, error) {
	if value, ok := s.memTable.Read(key); ok {
		internal.Logger.Debug("Read from memtable", "key", key, "value", value, "ok", ok)
		return value, nil
	}

	sstable := s.ssTableManager.FindByKey(key)

	if sstable == nil {
		internal.Logger.Debug("Failed to find sstable", "key", key)
		return nil, fmt.Errorf("sstable not found: %s", key)
	}

	record, err := s.ssTableManager.Read(sstable, key)

	if err != nil {
		internal.Logger.Debug("Read from sstable", "sstable", sstable.Path, "key", key, "value", record)
	} else {
		internal.Logger.Debug("Failed to read from sstable", "sstable", sstable.Path, "key", key, "value", record)
	}

	return record.Value, err
}

func (s *LSMTStorage) Iter(yield func(key string, value []byte) bool) {
	var keys []string

	for el := range s.memTable.Iterator() {
		keys = append(keys, el.Key)
		if !yield(el.Key, el.Value) {
			return
		}
	}

	for level := range s.ssTableManager.sstables {
		for _, sstable := range s.ssTableManager.sstables[level] {
			for _, key := range sstable.AllKeys() {
				if !slices.Contains(keys, key) {
					record, err := s.ssTableManager.Read(sstable, key)
					if err == nil {
						if !yield(key, record.Value) {
							return
						}
					}
				}
			}
		}
	}
}
