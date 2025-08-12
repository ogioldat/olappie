package olappie

import (
	"fmt"
	"os"
)

type WAL struct {
	file *os.File
}

func (w *WAL) Log(key, value string) error {
	entry := fmt.Sprintf("%s:%s\n", key, value)
	_, err := w.file.WriteString(entry)
	return err
}
