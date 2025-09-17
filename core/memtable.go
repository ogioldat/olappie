package core

import (
	"fmt"
	"io"
	"strings"

	"github.com/ogioldat/olappie/algo"
)

type MemTable interface {
	Append(string, []byte) error
	Read(string) (data []byte, ok bool)
	Flush(io.Writer) error
	Reset()
	Size() int
	Last() *algo.Node
	First() *algo.Node
	Serialize(SerializableNode) ([]byte, error)
	Iterator() <-chan *algo.Node
}

type SerializableNode struct {
	Key       string
	Value     []byte
	Timestamp int64
}

type RBMemTable struct {
	tree *algo.RBTree
}

func NewRBMemTable() *RBMemTable {
	return &RBMemTable{
		tree: algo.NewRBTree(),
	}
}

func NewFromKVPairs(kvStr string) (*RBMemTable, error) {
	memTable := NewRBMemTable()
	pairs := strings.Split(kvStr, ",")
	for _, kv := range pairs {
		kvParts := strings.SplitN(kv, ":", 2)
		if len(kvParts) == 2 {
			err := memTable.Append(kvParts[0], []byte(kvParts[1]))
			if err != nil {
				return nil, err
			}
		}
	}
	return memTable, nil
}

func (r *RBMemTable) Append(key string, value []byte) error {
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

func (r *RBMemTable) Serialize(data SerializableNode) ([]byte, error) {
	return fmt.Appendf(nil, "%s,%s,%d\n", data.Key, data.Value, data.Timestamp), nil
}

func (r *RBMemTable) Flush(w io.Writer) error {
	serializedData := []byte{}

	for node := range r.tree.StreamInorderTraversal() {
		serializedNode, err := r.Serialize(SerializableNode{
			Key:       node.Key,
			Value:     node.Value,
			Timestamp: node.Metadata.Timestamp.Unix(),
		})
		if err != nil {
			return err
		}
		serializedData = append(serializedData, serializedNode...)
	}

	if _, err := w.Write(serializedData); err != nil {
		return err
	}

	return nil
}

func (r *RBMemTable) Reset() {
	r.tree = algo.NewRBTree()
}

func (r *RBMemTable) Size() int {
	return r.tree.NodesCount
}

func (r *RBMemTable) Last() *algo.Node {
	return r.tree.Last()
}

func (r *RBMemTable) First() *algo.Node {
	return r.tree.First()
}

func (r *RBMemTable) Iterator() <-chan *algo.Node {
	return r.tree.StreamInorderTraversal()
}
