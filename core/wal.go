package core

import (
	"fmt"
	"os"
	"path"
)

type WAL struct {
	file      *os.File
	outputDir string
}

func (w *WAL) Log(key, value string) error {
	entry := fmt.Sprintf("%s:%s\n", key, value)
	_, err := w.file.WriteString(entry)
	return err
}

func NewWAL(config *LSMTStorageConfig) (*WAL, error) {
	file, err := os.OpenFile(
		path.Join(config.outputDir, "wal.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{file: file, outputDir: path.Join(config.outputDir, "wal.log")}, nil
}
