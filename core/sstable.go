package core

import (
	"fmt"
	"os"
	"path"
)

const DEFAULT_SSTABLE_DIR = "../data/sstables"
const DEFAULT_SSTABLE_SIZE = 1024 * 1024 // 1 MB

type SSTable struct {
	size  int
	level int
	name  string
	path  string
}

func (s *SSTable) Write(p []byte) (n int, err error) {
	dir := path.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, err
	}

	file, err := os.Create(s.path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return file.Write(p)
}

type Option func(*SSTableManager)

func WithSSTableDir(dir string) Option {
	return func(m *SSTableManager) {
		m.outputDir = dir
	}
}

type SSTableManager struct {
	sstables  map[int][]*SSTable
	outputDir string
}

func NewSSTableManager(opts ...Option) *SSTableManager {
	manager := &SSTableManager{
		sstables:  make(map[int][]*SSTable),
		outputDir: DEFAULT_SSTABLE_DIR,
	}

	for _, opt := range opts {
		opt(manager)
	}

	return manager
}

func (m *SSTableManager) FilePath(name string, level int) string {
	return path.Join(m.outputDir, "level_"+fmt.Sprint(level), name+".sst")
}

func (m *SSTableManager) AddSSTable() *SSTable {
	level := 0
	nextName := fmt.Sprintf("%04d", len(m.sstables[level])+1)
	sstable := &SSTable{
		size:  DEFAULT_SSTABLE_SIZE,
		level: level,
		name:  nextName,
		path:  m.FilePath(nextName, level),
	}
	m.sstables[level] = append(m.sstables[level], sstable)

	return sstable
}
