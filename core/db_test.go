package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBRead(t *testing.T) {
	tempDir := t.TempDir()

	db := NewLSMTStorage(WithOutDir(tempDir), WithMemtableThreshold(3))

	db.Write("a", []byte("data"))
	db.Write("b", []byte("data"))
	db.Write("c", []byte("data"))
	db.Write("d", []byte("data"))

	assert.Equal(t, 1, db.memTable.Size())
	assert.Equal(t, db.memTable.First().Key, "d")
}
