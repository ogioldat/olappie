package core

import (
	"fmt"
	"io"

	"github.com/ogioldat/olappie/algo"
)

type MemTable interface {
	Write(string, []byte) error
	Flush(io.Writer) error
	Size() int
}

type RBMemTable struct {
	tree *algo.RBTree
}

func NewRBMemTable() *RBMemTable {
	return &RBMemTable{
		tree: algo.NewRBTree(),
	}
}

func (r *RBMemTable) Write(key string, value []byte) error {
	return nil
}

func (r *RBMemTable) Flush(w io.Writer) error {
	for kv := range r.tree.StreamInorderTraversal() {
		kvStr := fmt.Sprint(kv.Key) + ":" + fmt.Sprint(kv.Value) + "\n"
		w.Write([]byte(kvStr))
	}
	return nil
}

func (r *RBMemTable) Size() int {
	// TODO: Implement size calculation
	return 0
}
