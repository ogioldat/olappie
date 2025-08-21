package core

import (
	"fmt"
	"io"
	"strings"

	"github.com/ogioldat/olappie/algo"
)

type MemTable interface {
	Write(string, []byte) error
	Read(string) (data []byte, ok bool)
	Flush(io.Writer) error
	Reset()
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

func NewFromKVPairs(kvStr string) *RBMemTable {
	memTable := NewRBMemTable()
	pairs := strings.Split(kvStr, ",")
	for _, kv := range pairs {
		kvParts := strings.SplitN(kv, ":", 2)
		if len(kvParts) == 2 {
			memTable.Write(kvParts[0], []byte(kvParts[1]))
		}
	}
	return memTable
}

func (r *RBMemTable) Write(key string, value []byte) error {
	r.tree.Insert(key, value)
	return nil
}

func (r *RBMemTable) Read(key string) (data []byte, ok bool) {
	value := r.tree.Search(key)
	if value != nil {
		return fmt.Append(nil, value.Value), true
	}
	return nil, false
}

func (r *RBMemTable) Flush(w io.Writer) error {
	for kv := range r.tree.StreamInorderTraversal() {
		kvStr := fmt.Sprint(kv.Key) + ":" + fmt.Sprint(kv.Value) + "\n"
		w.Write([]byte(kvStr))
	}
	return nil
}

func (r *RBMemTable) Reset() {
	r.tree = algo.NewRBTree()
}

func (r *RBMemTable) Size() int {
	return r.tree.NodesCount
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
