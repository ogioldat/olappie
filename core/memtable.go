package core

import (
	"fmt"
	"io"

	"github.com/ogioldat/olappie/algo"
)

type MemTable interface {
	Write(string, []byte) error
	Read(string) ([]byte, error)
	Flush(io.Writer) error
	Size() int
	Last() *algo.KVPair
	First() *algo.KVPair
	Iterator() <-chan *algo.KVPair
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

func (r *RBMemTable) Read(key string) ([]byte, error) {
	value := r.tree.Search(key)
	return []byte(fmt.Sprint(value.Value)), nil
}

func (r *RBMemTable) Flush(w io.Writer) error {
	for kv := range r.tree.StreamInorderTraversal() {
		kvStr := fmt.Sprint(kv.Key) + ":" + fmt.Sprint(kv.Value) + "\n"
		w.Write([]byte(kvStr))
	}
	r.tree = algo.NewRBTree()

	return nil
}

func (r *RBMemTable) Size() int {
	// TODO: Implement size calculation
	return 0
}

func (r *RBMemTable) Last() *algo.KVPair {
	return r.tree.Last()
}

func (r *RBMemTable) First() *algo.KVPair {
	return r.tree.First()
}

func (r *RBMemTable) Iterator() <-chan *algo.KVPair {
	return r.tree.StreamInorderTraversal()
}
