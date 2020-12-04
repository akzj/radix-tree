package rtree

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

type FreeList struct {
	mx    sync.Mutex
	nodes []*node
	size  int
}

type CopyOnWriteContext struct {
	freelist *FreeList
}

type children []*node

type node struct {
	key      bool
	cow      *CopyOnWriteContext
	children children
	prefix   []byte
}

type RTree struct {
	cow      *CopyOnWriteContext
	children children
}

func NewFreeList(size int) *FreeList {
	return &FreeList{
		mx:    sync.Mutex{},
		nodes: make([]*node, 0, size),
		size:  size,
	}
}

func (c *CopyOnWriteContext) newNode() *node {
	n := c.freelist.newNode()
	n.cow = c
	return n
}

func (freelist *FreeList) newNode() *node {
	var n *node
	freelist.mx.Lock()
	if size := len(freelist.nodes); size != 0 {
		n = freelist.nodes[size-1]
		freelist.nodes[size-1] = nil
		freelist.nodes = freelist.nodes[:size-1]
	}
	freelist.mx.Unlock()
	if n == nil {
		return new(node)
	}
	return n
}

func (freelist *FreeList) freeNode(node *node) {
	freelist.mx.Lock()
	if size := len(freelist.nodes); size < freelist.size {
		freelist.nodes = append(freelist.nodes, node)
	}
	freelist.mx.Lock()
}

var DefaultFreeListSize = 32

func New() *RTree {
	return &RTree{
		cow: &CopyOnWriteContext{freelist: NewFreeList(DefaultFreeListSize)},
	}
}

func (tree *RTree) Clone() *RTree {
	clone := *tree
	cow1, cow2 := *tree.cow, *tree.cow
	clone.children = make(children, len(tree.children))
	copy(clone.children, tree.children)
	clone.cow = &cow1
	tree.cow = &cow2
	return &clone
}

func newRNode(cow *CopyOnWriteContext, prefix []byte, key bool) *node {
	n := cow.newNode()
	n.prefix = prefix
	n.key = key
	return n
}

func (children children) Print() {
	for _, child := range children {
		fmt.Println(child.prefix)
	}
}

func (children children) findNode(first byte) (int, *node) {
	i := sort.Search(len(children), func(i int) bool {
		return first < children[i].prefix[0]
	})
	if i > 0 && children[i-1].prefix[0] == first {
		return i - 1, children[i-1]
	}
	return i, nil
}

func (children *children) insetAt(node *node, index int) {
	*children = append(*children, nil)
	if index < len(*children) {
		copy((*children)[index+1:], (*children)[index:])
	}
	(*children)[index] = node
}

func (children *children) deleteAt(index int) {
	copy((*children)[index:], (*children)[index+1:])
	(*children)[len(*children)-1] = nil
	*children = (*children)[:len(*children)-1]
}

func (children *children) delete(cow *CopyOnWriteContext, key []byte) {
	if index, child := children.findNode(key[0]); child != nil {
		child = children.mutableChild(cow, index)
		if len(key) < len(child.prefix) ||
			bytes.Compare(key[:len(child.prefix)], child.prefix) != 0 {
			return
		}
		if len(child.prefix) == len(key) {
			if len(child.children) == 0 {
				children.deleteAt(index)
				child.prefix = nil
			} else {
				if len(child.children) == 1 {
					child.merge()
				} else {
					child.key = false
				}
			}
			return
		}
		if len(child.children) == 0 {
			return
		}
		child.children.delete(cow, key[len(child.prefix):])
		if len(child.children) == 1 && child.key == false {
			child.merge()
		}
	}
}

func (children *children) mutableChild(cow *CopyOnWriteContext, index int) *node {
	c := (*children)[index]
	if c.cow != cow {
		c = c.mutableFor(cow)
		(*children)[index] = c
	}
	return c
}

func (children children) walk(stack [][]byte, f func(prefixes [][]byte) bool) bool {
	for _, child := range children {
		if child.key {
			if f(append(stack, child.prefix)) == false {
				return false
			}
		}
		if child.children.walk(append(stack, child.prefix), f) == false {
			return false
		}
	}
	return true
}

func (n *node) find(key []byte) bool {
	if n.key && bytes.Compare(n.prefix, key) == 0 {
		return true
	}
	key = key[len(n.prefix):]
	if _, child := n.children.findNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func (tree *RTree) Find(key []byte) bool {
	if tree.children == nil || len(key) == 0 {
		return false
	}
	if _, child := tree.children.findNode(key[0]); child == nil {
		return false
	} else {
		return child.find(key)
	}
}

func bytesCopy(data []byte) []byte {
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func (tree *RTree) Insert(key []byte) {
	if len(key) == 0 {
		return
	}
	index, child := tree.children.findNode(key[0])
	if child == nil {
		tree.children.insetAt(newRNode(tree.cow, key, true), index)
	} else {
		tree.children.mutableChild(tree.cow, index).insert(key)
	}
}

func (tree *RTree) Delete(key []byte) {
	tree.children.delete(tree.cow, key)
}

func (tree RTree) Walk(f func(prefixes [][]byte) bool) {
	tree.children.walk(make([][]byte, 0, 32), f)
}

func prefixLen(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func (n *node) mutableFor(cow *CopyOnWriteContext) *node {
	if n.cow == cow {
		return n
	}
	out := cow.newNode()
	if cap(out.children) >= len(n.children) {
		out.children = out.children[:len(n.children)]
	} else {
		out.children = make(children, len(n.children), cap(n.children))
	}
	if len(out.children) > 0 {
		copy(out.children, n.children)
	}
	out.prefix = bytesCopy(n.prefix)
	out.key = n.key
	return out
}

func (n *node) mutableChild(i int) *node {
	c := n.children[i].mutableFor(n.cow)
	n.children[i] = c
	return c
}

func printTokens(bytesTokens [][]byte) {
	var tokens []string
	for _, token := range bytesTokens {
		tokens = append(tokens, string(token))
	}
	fmt.Println(tokens)
}

func (n *node) insert(key []byte) {
	if bytes.Compare(n.prefix, key) == 0 {
		if n.key == false {
			n.key = true
		} else {
			//fmt.Println("repeated")
		}
		return
	}
	index := prefixLen(n.prefix, key)
	if index == len(n.prefix) {
		key = key[index:]
		index, child := n.children.findNode(key[0])
		if child == nil {
			n.children.insetAt(newRNode(n.cow, bytesCopy(key), true), index)
		} else {
			n.mutableChild(index).insert(key)
		}
	} else {
		child := *n
		child.prefix = n.prefix[index:]
		n.key = false
		n.prefix = n.prefix[:index]
		n.children = make(children, 1, 2)
		n.children[0] = &child
		key = key[index:]
		if len(key) > 0 {
			index, _ := n.children.findNode(key[0])
			n.children.insetAt(newRNode(n.cow, bytesCopy(key), true), index)
		} else {
			n.key = true
		}
	}
}

func (n *node) merge() {
	prefix := make([]byte, len(n.prefix)+len(n.children[0].prefix))
	n.key = n.children[0].key
	copy(prefix, n.prefix)
	copy(prefix[len(n.prefix):], n.children[0].prefix)
	n.prefix = prefix
	old := n.children
	n.children = n.children[0].children
	old[0] = nil
}

func (n *node) findNode(b byte) (int, *node) {
	return n.children.findNode(b)
}

type stackItem struct {
	*node
	visit bool
}

type stack struct {
	stack []*stackItem
}

func (s *stack) peek() *stackItem {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	return rNode
}

func (s *stack) pop() *stackItem {
	if len(s.stack) == 0 {
		return nil
	}
	rNode := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return rNode
}

func (s *stack) push(children ...*node) {
	for i := len(children) - 1; i >= 0; i-- {
		s.stack = append(s.stack, &stackItem{node: children[i]})
	}
}

const (
	PushKey = '='
	Push    = '+'
	Pop     = '-'
)

func (tree *RTree) WriteTo(writer io.Writer) (int64, error) {
	var size int64
	var stack stack
	var text bytes.Buffer
	var popText = []byte{Pop, '\n'}
	stack.push(tree.children...)
	for item := stack.peek(); item != nil; item = stack.peek() {
		visit := item.visit
		item.visit = true
		if visit == false {
			text.Reset()
			if item.key {
				text.WriteByte(PushKey)
			} else {
				text.WriteByte(Push)
			}
			text.Write(item.prefix)
			text.WriteByte('\n')
			if n, err := writer.Write(text.Bytes()); err != nil {
				return 0, err
			} else {
				size += int64(n)
			}
			if item.children != nil {
				stack.push(item.children...)
				continue
			}
		}
		stack.pop()
		if n, err := writer.Write(popText); err != nil {
			return 0, err
		} else {
			size += int64(n)
		}
	}
	return size, nil
}

func FastBuildTree(reader io.Reader) (*RTree, error) {
	const bufferSize = 1 << 10
	var tree = New()
	var stack = make([]*children, 0, 128)
	var curr *children
	var scanner = bufio.NewScanner(reader)
	var tokensCh = make(chan [][]byte, 4<<10)
	var buffer = make([][]byte, 0, bufferSize)

	go func() {
		for scanner.Scan() {
			data := scanner.Bytes()
			k := make([]byte, len(data))
			copy(k, data)
			buffer = append(buffer, k)
			if len(buffer) < bufferSize {
				continue
			}
			tokensCh <- buffer
			buffer = make([][]byte, 0, bufferSize)
		}
		tokensCh <- buffer
		close(tokensCh)
	}()
	for tokens := range tokensCh {
		for _, token := range tokens {
			if token[0] == PushKey || token[0] == Push {
				if len(stack) == 0 {
					stack = append(stack, &tree.children)
					curr = &tree.children
				} else {
					stack = append(stack, curr)
				}
				if curr == nil {
					return nil, fmt.Errorf("stack error")
				}
				next := newRNode(tree.cow, token[1:], token[0] == PushKey)
				*curr = append(*curr, next)
				curr = &next.children
			} else if token[0] == Pop {
				if len(stack) == 0 {
					return nil, fmt.Errorf("stack error")
				}
				curr = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			} else {
				return nil, fmt.Errorf("unkown token %c", token[0])
			}
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("broken stack")
	}
	return tree, nil

}

func BuildTree(reader io.Reader) (*RTree, error) {
	var tree RTree
	var stack []string
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		text := scanner.Text()
		if text[0] == PushKey || text[0] == Push {
			stack = append(stack, text[1:])
			if text[0] == PushKey {
				tree.Insert([]byte(strings.Join(stack, "")))
			}
		} else if text[0] == Pop {
			if len(stack) == 0 {
				return nil, fmt.Errorf("stack error")
			}
			stack = stack[:len(stack)-1]
		} else {
			return nil, fmt.Errorf("unkown token %c", text[0])
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("broken stack")
	}
	return &tree, nil
}
