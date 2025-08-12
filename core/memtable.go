package core

import (
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

func (r *RBMemTable) Write(key string, value []byte) error {
	return nil
}

func (r *RBMemTable) Flush(w io.Writer) error {
	for kv := range r.tree.StreamInorderTraversal() {
		kvStr := string(kv.Key) + ":" + string(kv.Value) + "\n"
		w.Write([]byte(kvStr))
	}
	return nil
}

func (r *RBMemTable) Size() int {
	// TODO: Implement size calculation
	return 0
}
