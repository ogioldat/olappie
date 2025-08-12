package olappie

import (
	"fmt"
	"os"
)

const WAL_PATH = "data/wal.log"

type WAL struct {
	file *os.File
}

func (w *WAL) Log(key, value string) error {
	entry := fmt.Sprintf("%s:%s\n", key, value)
	_, err := w.file.WriteString(entry)
	return err
}

func NewWAL() *WAL {
	file, err := os.OpenFile(WAL_PATH, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to open WAL file: %v", err))
	}
	return &WAL{file: file}
}
