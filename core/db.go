package core

import (
	"fmt"
	"slices"

	"github.com/ogioldat/olappie/internal"
)

const DEFAULT_OUTPUT_DIR = "../data/"

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

func WithMemtableThreshold(th int) Option {
	return func(m *LSMTStorageConfig) {
		m.memTableThreshold = th
	}
}

type LSMTStorageConfig struct {
	memTableThreshold int // Max size of entries in the memtable before flushing to SSTables
	outputDir         string
}

type LSMTStorage struct {
	config         *LSMTStorageConfig
	seqNumber      int
	sparseIndex    SparseIndex
	memTable       MemTable
	ssTableManager *SSTableManager
	wal            *WAL
	metadata       *Metadata
}

func NewLSMTStorage(opts ...Option) *LSMTStorage {
	config := &LSMTStorageConfig{
		memTableThreshold: 1000,
		outputDir:         DEFAULT_OUTPUT_DIR,
	}

	for _, opt := range opts {
		opt(config)
	}

	wal, err := NewWAL(config)
	if err != nil {
		panic("failed to create WAL")
	}

	return &LSMTStorage{
		config:         config,
		seqNumber:      0,
		sparseIndex:    NewSparseIndex(),
		memTable:       NewRBMemTable(),
		ssTableManager: NewSSTableManager(config),
		wal:            wal,
		metadata:       NewMetadata(),
	}
}

func (s *LSMTStorage) updateSeq() {
	s.seqNumber++
}

func (s *LSMTStorage) Write(key string, value []byte) error {
	if err := s.wal.Log(key, string(value)); err != nil {
		internal.Logger.Debug("WAL log failed", "key", key, "value", value, "err", err)
		return err
	}

	if err := s.memTable.Write(key, []byte(value)); err != nil {
		internal.Logger.Debug("Memtable write failed", "key", key, "value", value, "err", err)
		return err
	}

	internal.Logger.Debug("Write to memtable", "key", key, "value", value)

	s.updateSeq()

	// TODO: Move as a background task
	if s.config.memTableThreshold <= s.memTable.Size() {
		sstable := s.ssTableManager.AddSSTable(s.config)

		// Populate blooms filter
		for kv := range s.memTable.Iterator() {
			sstable.BloomFilter.Add(kv.Key)
		}

		memtableHead := s.memTable.First()
		memtableTail := s.memTable.Last()

		s.metadata.Set(
			sstable.Name,
			memtableHead.Key,
			memtableTail.Key,
			sstable.Level,
		)

		if err := s.memTable.Flush(sstable); err != nil {
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

	sstable := s.ssTableManager.FindByKey(key, s.metadata)

	if sstable == nil {
		internal.Logger.Debug("Failed to find sstable", "key", key)
		return nil, fmt.Errorf("sstable not found: %s", key)
	}

	value, err := sstable.Read(key)

	if err != nil {
		internal.Logger.Debug("Read from sstable", "sstable", sstable.Id, "key", key, "value", value)
	} else {
		internal.Logger.Debug("Failed to read from sstable", "sstable", sstable.Id, "key", key, "value", value)
	}

	return value, err
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
					value, err := sstable.Read(key)
					if err == nil {
						if !yield(key, value) {
							return
						}
					}
				}
			}
		}
	}
}
