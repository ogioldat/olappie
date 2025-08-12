package core

import (
	"os"
	"path"
)

const SSTABLE_DIR = "data/sstables/"
const DEFAULT_SSTABLE_SIZE = 1024 * 1024 // 1 MB

type SSTable struct {
	size  int
	level int
	name  string
}

func getSSTablePath(level int) string {
	return path.Join(SSTABLE_DIR, "level_"+string(level)+".sst")
}

func NewSSTable() *SSTable {
	level := 0
	tablePath := getSSTablePath(0)

	f, err := os.Create(tablePath)
	if err != nil {
		return nil
	}
	defer f.Close()

	return &SSTable{
		size:  DEFAULT_SSTABLE_SIZE,
		level: level,
		name:  tablePath,
	}
}

func (s *SSTable) FileName() string {
	return getSSTablePath(s.level)
}

func (s *SSTable) Write(p []byte) (n int, err error) {
	file, err := os.OpenFile(s.FileName(), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return file.Write(p)
}
