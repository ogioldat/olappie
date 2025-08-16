package algo

const (
	RED   = true
	BLACK = false
)

type Node struct {
	Key    string
	Value  int
	Color  bool
	Left   *Node
	Right  *Node
	Parent *Node
}

type KVPair struct {
	Key   string
	Value int
}

type RBTree struct {
	Root       *Node
	NodesCount int
}

func (t *RBTree) Search(key string) *Node {
	n := t.Root
	for n != nil {
		if key < n.Key {
			n = n.Left
		} else if key > n.Key {
			n = n.Right
		} else {
			return n
		}
	}
	return nil
}

func (t *RBTree) Insert(key string) {
	newNode := &Node{Key: key, Color: RED}
	var parent *Node
	n := t.Root

	for n != nil {
		parent = n
		if key < n.Key {
			n = n.Left
		} else {
			n = n.Right
		}
	}

	newNode.Parent = parent
	if parent == nil {
		t.Root = newNode
	} else if key < parent.Key {
		parent.Left = newNode
	} else {
		parent.Right = newNode
	}

	t.fixInsert(newNode)
}

func (t *RBTree) fixInsert(n *Node) {
	for n != t.Root && n.Parent.Color == RED {
		if n.Parent == n.Parent.Parent.Left {
			uncle := n.Parent.Parent.Right
			if uncle != nil && uncle.Color {
				// Case 1: Uncle is red
				n.Parent.Color = BLACK
				uncle.Color = BLACK
				n.Parent.Parent.Color = RED
				n = n.Parent.Parent
			} else {
				if n == n.Parent.Right {
					// Case 2: Uncle black, triangle
					n = n.Parent
					t.rotateLeft(n)
				}
				// Case 3: Uncle black, line
				n.Parent.Color = BLACK
				n.Parent.Parent.Color = RED
				t.rotateRight(n.Parent.Parent)
			}
		} else {
			uncle := n.Parent.Parent.Left
			if uncle != nil && uncle.Color == RED {
				// Mirror Case 1
				n.Parent.Color = BLACK
				uncle.Color = BLACK
				n.Parent.Parent.Color = RED
				n = n.Parent.Parent
			} else {
				if n == n.Parent.Left {
					// Mirror Case 2
					n = n.Parent
					t.rotateRight(n)
				}
				// Mirror Case 3
				n.Parent.Color = BLACK
				n.Parent.Parent.Color = RED
				t.rotateLeft(n.Parent.Parent)
			}
		}
	}
	t.Root.Color = BLACK
}

func (t *RBTree) rotateLeft(x *Node) {
	y := x.Right
	x.Right = y.Left
	if y.Left != nil {
		y.Left.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}
	y.Left = x
	x.Parent = y
}

func (t *RBTree) rotateRight(x *Node) {
	y := x.Left
	x.Left = y.Right
	if y.Right != nil {
		y.Right.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Right {
		x.Parent.Right = y
	} else {
		x.Parent.Left = y
	}
	y.Right = x
	x.Parent = y
}

func NewRBTree() *RBTree {
	return &RBTree{
		Root:       nil,
		NodesCount: 0,
	}
}

func (node *Node) inorderTraversal(sortedOut chan<- *KVPair) {
	if node != nil {
		node.Left.inorderTraversal(sortedOut)
		sortedOut <- &KVPair{Key: node.Key, Value: node.Value}
		node.Right.inorderTraversal(sortedOut)
	}
}

func (tree *RBTree) StreamInorderTraversal() <-chan *KVPair {
	sortedOut := make(chan *KVPair)

	go func() {
		defer close(sortedOut)
		tree.Root.inorderTraversal(sortedOut)
	}()
	return sortedOut
}

func (tree *RBTree) First() *KVPair {
	if tree.Root == nil {
		return nil
	}

	return &KVPair{Key: tree.Root.Key, Value: tree.Root.Value}
}

func getLast(node *Node) *Node {
	if node == nil {
		return nil
	}
	for node.Right != nil {
		node = node.Right
	}
	return node
}

func (tree *RBTree) Last() *KVPair {
	if tree.Root == nil {
		return nil
	}
	lastNode := getLast(tree.Root)
	return &KVPair{Key: lastNode.Key, Value: lastNode.Value}
}
