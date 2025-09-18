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
	walDir := path.Join(config.outputDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}
	walPath := path.Join(walDir, "wal.log")
	file, err := os.Create(walPath)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file, outputDir: walDir}, nil
}
